I. Thực hành xây dựng SeqNeuMF cơ bản
Trong phần này, chúng ta thực hành một pipeline hệ thống gợi ý tối giản trên dữ liệu tương tác video. Mục tiêu của bài thực hành là giúp người học nắm được toàn bộ quy trình bao gồm từ việc tải dữ liệu, tiền xử lý, xây dựng kiến trúc mô hình SeqNeuMF kết hợp đặc trưng hình ảnh, cho đến vòng lặp huấn luyện, đánh giá và gợi ý video thực tế.

Kiến trúc mô hình trong bài thực hành được lấy cảm hứng từ hai công trình nền tảng là [Neural Collaborative Filtering](https://arxiv.org/abs/1708.05031) và [Self-Attentive Sequential Recommendation](https://arxiv.org/abs/1808.09781). Toàn bộ pipeline được xây dựng và đánh giá trên tập dữ liệu nền tảng dành cho video ngắn [MicroLens](https://arxiv.org/abs/2309.15379).

<p align="center">
  <img src="../assets/m-seqmf.png" alt="Kiến trúc SeqNeuMF" width="80%">
  <br>
  Hình 1. Sơ đồ kiến trúc tổng quan của mô hình SeqNeuMF.
</p>

> **Lưu ý -** Hãy chạy các khối mã hay cell theo đúng thứ tự. Nếu bạn dùng GPU của Colab, thời gian huấn luyện sẽ được rút ngắn.


Bước 1 - Tải dữ liệu giả định
Khối này hỗ trợ tải bộ dataset về môi trường Colab bao gồm `pairs.csv`, `visual_embeddings.pt`, `titles.csv`. Sau khi chạy, thư mục `/content/dataset/` sẽ chứa các file dữ liệu cần thiết để chuẩn bị cho quá trình xây dựng và huấn luyện mô hình.

```bash
# Ví dụ tải data (bỏ comment để chạy nếu cần)
# !mkdir -p /content/dataset
# !wget -q -O /content/dataset/pairs.csv <URL>
# !wget -q -O /content/dataset/visual_embeddings.pt <URL>
# !wget -q -O /content/dataset/titles.csv <URL>
```

Bước 2 - Cài đặt dependencies
Khối tiếp theo cài đặt thư viện `tensorboardX` để hỗ trợ ghi log, đồng thời tạo sẵn thư mục `checkpoints` để hệ thống lưu lại weights của mô hình đạt kết quả cao trong quá trình huấn luyện.

```python
!pip install -q tensorboardX
!mkdir -p /content/checkpoints
```

Bước 3 - Import thư viện và thiết lập cấu hình
Phần này import toàn bộ các thư viện cần thiết và cấu hình hyper-parameters. Tại đây, ta cố định các thông số huấn luyện như batch size, learning rate, số lượng layer, tuỳ chọn kiến trúc như dùng GPU hay không và khởi tạo seed để dễ dàng tái tạo lại kết quả.

```python
import os, math, random
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from copy import deepcopy
from torch.utils.data import DataLoader, Dataset
from tqdm.auto import tqdm

random.seed(0)
torch.manual_seed(0)

# ── PATHS ────────────────────────────────────────────────────────────────────
DATA_DIR       = "/content/dataset"
CHECKPOINT_DIR = "/content/checkpoints"

# ── HYPER-PARAMS ─────────────────────────────────────────────────────────────
config = {
    'alias'               : 'seqneumf_visual',
    'num_epoch'           : 5,
    'batch_size'          : 1024,
    'optimizer'           : 'adam',
    'adam_lr'             : 1e-3,
    'l2_regularization'   : 1e-7,
    'latent_dim_mf'       : 8,
    'latent_dim_mlp'      : 8,
    'layers'              : [16, 64, 32, 16, 8],   # layers[0] = latent_mlp*2
    'num_negative'        : 4,
    'visual_dim'          : 768,                   # 0 để tắt visual
    'use_seq_user'        : True,
    'maxlen'              : 50,
    'seq_hidden_units'    : 50,
    'num_heads'           : 1,
    'num_blocks'          : 2,
    'dropout_rate'        : 0.2,
    'weight_init_gaussian': True,
    'use_cuda'            : True,
    'use_bachify_eval'    : True,
    'device_id'           : 0,
    'model_dir'           : os.path.join(CHECKPOINT_DIR, '{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'),
}

DEVICE = torch.device('cuda' if config['use_cuda'] and torch.cuda.is_available() else 'cpu')
VISUAL_DIM = config['visual_dim']
print("Device:", DEVICE)
```

Bước 4 - Tải và tiền xử lý dữ liệu tương tác
Đọc tệp tương tác `pairs.csv` giữa người dùng và video. Sau đó reindex chuỗi ID gốc thành các số nguyên liên tục từ `0` đến `N-1` để tiện đưa vào các lớp Embedding của PyTorch, và in ra số lượng user và item tổng quát.

```python
interactions = pd.read_csv(os.path.join(DATA_DIR, 'pairs.csv'))
interactions['rating'] = 1.0

# Reindex userId
user_id_map = interactions[['user']].drop_duplicates().reset_index(drop=True)
user_id_map['userId'] = np.arange(len(user_id_map))
interactions = pd.merge(interactions, user_id_map, on='user', how='left')

# Reindex itemId
item_id_map = interactions[['item']].drop_duplicates().reset_index(drop=True)
item_id_map['itemId'] = np.arange(len(item_id_map))
interactions = pd.merge(interactions, item_id_map, on='item', how='left')

ml_ratings = interactions[['userId', 'itemId', 'rating', 'timestamp']]

print(f"Users : {ml_ratings.userId.nunique()}  (range {ml_ratings.userId.min()}–{ml_ratings.userId.max()})")
print(f"Items : {ml_ratings.itemId.nunique()}  (range {ml_ratings.itemId.min()}–{ml_ratings.itemId.max()})")
print(f"Rows  : {len(ml_ratings)}")

config['num_users'] = ml_ratings['userId'].nunique()
config['num_items'] = ml_ratings['itemId'].nunique()
```

Bước 5 - Nạp đặc trưng hình ảnh
Nạp các vector đặc trưng hình ảnh `visual_embeddings.pt` đã được trích xuất sẵn từ cover của các video. Các ID video tại đây cũng được chuyển đổi sang ID mới đồng bộ với khối trước để mô hình tra cứu nhanh chóng.

```python
raw_visual   = torch.load(os.path.join(DATA_DIR, 'visual_embeddings.pt'), weights_only=False)
orig_to_new  = dict(zip(item_id_map['item'], item_id_map['itemId']))
visual_embeddings = {orig_to_new[k]: v for k, v in raw_visual.items() if k in orig_to_new}

print(f"Visual embeddings loaded: {len(visual_embeddings)} items")
sample_v = next(iter(visual_embeddings.values()))
print(f"Embedding dim: {sample_v.shape}")
```

Bước 6 - Khởi tạo Dataset và DataLoader
Định nghĩa các đối tượng `Dataset` và `DataLoader`. Khối này thực hiện bước chuẩn bị quan trọng: chia dữ liệu huấn luyện và đánh giá bằng leave-one-out, negative sampling, cũng như trích xuất sequence cho từng người dùng.

```python
class UserItemRatingDataset(Dataset):
    def __init__(self, user_tensor, seq_tensor, item_tensor, target_tensor, visual_embeddings):
        self.user_tensor      = user_tensor
        self.seq_tensor       = seq_tensor
        self.item_tensor      = item_tensor
        self.target_tensor    = target_tensor
        self.visual_embeddings = visual_embeddings

    def __getitem__(self, index):
        item_id = self.item_tensor[index].item()
        visual  = self.visual_embeddings.get(item_id, torch.zeros(VISUAL_DIM))
        return (self.user_tensor[index], self.seq_tensor[index],
                self.item_tensor[index], self.target_tensor[index], visual)

    def __len__(self):
        return self.user_tensor.size(0)


class VisualLookup:
    def __init__(self, items, visual_embeddings):
        self.items = items
        self.visual_embeddings = visual_embeddings

    def __getitem__(self, idx):
        ids = self.items[idx]
        if ids.dim() == 0:
            return self.visual_embeddings.get(ids.item(), torch.zeros(VISUAL_DIM))
        return torch.stack([self.visual_embeddings.get(i.item(), torch.zeros(VISUAL_DIM)) for i in ids])

    def __len__(self):
        return len(self.items)


class SampleGenerator:
    def __init__(self, ratings: pd.DataFrame, visual_embeddings: dict, maxlen: int = 50):
        assert {'userId', 'itemId', 'rating', 'timestamp'}.issubset(ratings.columns)
        self.ratings           = ratings
        self.visual_embeddings = visual_embeddings
        self.maxlen            = maxlen
        self.item_pool         = set(ratings['itemId'].unique())
        self.negatives         = self._sample_negative(ratings)
        self.train_ratings, self.test_ratings = self._split_loo(self._binarize(ratings))
        sorted_r = ratings.sort_values(['userId', 'timestamp'])
        self.user_history = sorted_r.groupby('userId')['itemId'].apply(list).to_dict()

    def _binarize(self, r):
        r = r.copy(); r.loc[r['rating'] > 0, 'rating'] = 1.0; return r

    def _split_loo(self, r):
        r['rank_latest'] = r.groupby('userId')['timestamp'].rank(method='first', ascending=False)
        return r[r['rank_latest'] > 1][['userId','itemId','rating']], r[r['rank_latest'] == 1][['userId','itemId','rating']]

    def _sample_negative(self, ratings):
        grp = ratings.groupby('userId')['itemId'].apply(set).reset_index()
        grp.columns = ['userId', 'interacted']
        grp['neg_items']   = grp['interacted'].apply(lambda x: self.item_pool - x)
        grp['neg_samples'] = grp['neg_items'].apply(lambda x: random.sample(list(x), 99))
        self._neg_items = dict(zip(grp['userId'], grp['neg_items']))
        return grp[['userId', 'neg_samples']]

    def _get_seq(self, uid, target_iid):
        hist = self.user_history.get(uid, [])
        try:
            idx = hist.index(target_iid); seq = hist[:idx]
        except ValueError:
            seq = hist
        seq = seq[-self.maxlen:]
        return [0] * (self.maxlen - len(seq)) + seq

    def instance_a_train_loader(self, num_negatives, batch_size):
        users, seqs, items, ratings = [], [], [], []
        for row in self.train_ratings.itertuples():
            uid, iid = int(row.userId), int(row.itemId)
            seq = self._get_seq(uid, iid)
            users.append(uid); seqs.append(seq); items.append(iid); ratings.append(1.0)
            for neg in random.sample(list(self._neg_items[uid]), num_negatives):
                users.append(uid); seqs.append(seq); items.append(int(neg)); ratings.append(0.0)
        ds = UserItemRatingDataset(
            torch.LongTensor(users), torch.LongTensor(seqs),
            torch.LongTensor(items), torch.FloatTensor(ratings),
            self.visual_embeddings)
        return DataLoader(ds, batch_size=batch_size, shuffle=True)

    @property
    def evaluate_data(self):
        test = pd.merge(self.test_ratings, self.negatives, on='userId')
        tu, tseq, ti = [], [], []
        nu, nseq, ni = [], [], []
        for row in test.itertuples():
            uid, iid = int(row.userId), int(row.itemId)
            hist = self.user_history.get(uid, [])
            try:
                idx = hist.index(iid); seq = hist[:idx]
            except:
                seq = hist[:-1]
            seq = seq[-self.maxlen:]
            padded = [0] * (self.maxlen - len(seq)) + seq
            tu.append(uid); tseq.append(padded); ti.append(iid)
            for neg in row.neg_samples:
                nu.append(uid); nseq.append(padded); ni.append(int(neg))
        ti_t = torch.LongTensor(ti); ni_t = torch.LongTensor(ni)
        return [
            torch.LongTensor(tu), torch.LongTensor(tseq), ti_t, VisualLookup(ti_t, self.visual_embeddings),
            torch.LongTensor(nu), torch.LongTensor(nseq), ni_t, VisualLookup(ni_t, self.visual_embeddings),
        ]


print("Building SampleGenerator (may take ~30s)...")
sample_generator = SampleGenerator(ml_ratings, visual_embeddings, maxlen=config['maxlen'])
evaluate_data    = sample_generator.evaluate_data
print("Done.")
```

Bước 7 - Xây dựng kiến trúc mô hình SeqNeuMF
Đây là khối khai báo lõi kiến trúc của hệ thống. Mô hình `SeqNeuMF` chia thành hai thành phần chính: `SequentialEncoder` để mô hình hóa chuỗi hành vi của người dùng, kết hợp với mạng NeuMF/GMF kết hợp Visual Fusion để đưa ra dự đoán sau cùng.

```python
class SequentialEncoder(nn.Module):
    """Transformer-based sequential user encoder (SASRec-style)."""

    def __init__(self, num_items, hidden_dim, maxlen, num_heads, num_layers, dropout):
        super().__init__()
        self.item_emb    = nn.Embedding(num_items + 1, hidden_dim, padding_idx=0)
        self.pos_emb     = nn.Embedding(maxlen + 1,    hidden_dim, padding_idx=0)
        self.emb_dropout = nn.Dropout(p=dropout)
        enc_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim, nhead=num_heads,
            dim_feedforward=hidden_dim, dropout=dropout,
            activation='relu', batch_first=True, norm_first=False)
        self.transformer = nn.TransformerEncoder(
            enc_layer, num_layers=num_layers,
            norm=nn.LayerNorm(hidden_dim, eps=1e-8))

    def forward(self, item_seq):
        device   = item_seq.device
        positions = torch.arange(1, item_seq.size(1)+1, device=device).unsqueeze(0).expand_as(item_seq)
        positions = positions * (item_seq != 0)
        x = self.item_emb(item_seq) * (self.item_emb.embedding_dim ** 0.5) + self.pos_emb(positions)
        x = self.emb_dropout(x)
        seq_len = x.size(1)
        causal_mask  = nn.Transformer.generate_square_subsequent_mask(seq_len, device=device)
        padding_mask = (item_seq == 0)
        x = self.transformer(x, mask=causal_mask, is_causal=True, src_key_padding_mask=padding_mask)
        return x[:, -1, :]   # (B, hidden_dim)


class SeqNeuMF(nn.Module):
    """SeqNeuMF: NeuMF + Transformer user encoder + visual fusion."""

    def __init__(self, config):
        super().__init__()
        self.use_seq_user   = config.get('use_seq_user', True)
        self.latent_dim_mf  = config['latent_dim_mf']
        self.latent_dim_mlp = config['latent_dim_mlp']
        hidden_seq          = config.get('seq_hidden_units', 50)

        # User branch
        if self.use_seq_user:
            self.seq_encoder   = SequentialEncoder(
                config['num_items'], hidden_seq,
                config.get('maxlen', 50), config.get('num_heads', 1),
                config.get('num_blocks', 2), config.get('dropout_rate', 0.2))
            self.proj_user_mf  = nn.Linear(hidden_seq, self.latent_dim_mf)
            self.proj_user_mlp = nn.Linear(hidden_seq, self.latent_dim_mlp)
        else:
            self.user_emb_mf  = nn.Embedding(config['num_users'], self.latent_dim_mf)
            self.user_emb_mlp = nn.Embedding(config['num_users'], self.latent_dim_mlp)

        # Item branch
        self.item_emb_mf  = nn.Embedding(config['num_items'], self.latent_dim_mf)
        self.item_emb_mlp = nn.Embedding(config['num_items'], self.latent_dim_mlp)

        # Visual fusion
        visual_dim = config.get('visual_dim', 768)
        self.use_visual = visual_dim > 0
        if self.use_visual:
            self.visual_fusion_mf  = nn.Linear(self.latent_dim_mf  + visual_dim, self.latent_dim_mf)
            self.visual_fusion_mlp = nn.Linear(self.latent_dim_mlp + visual_dim, self.latent_dim_mlp)

        # MLP tower
        self.fc_layers = nn.ModuleList([
            nn.Linear(i, o) for i, o in zip(config['layers'][:-1], config['layers'][1:])])

        # Output
        self.output_layer = nn.Linear(config['layers'][-1] + self.latent_dim_mf, 1)
        self.sigmoid = nn.Sigmoid()

        if config.get('weight_init_gaussian', False):
            for m in self.modules():
                if isinstance(m, (nn.Embedding, nn.Linear)):
                    nn.init.normal_(m.weight.data, mean=0.0, std=0.01)

    def forward(self, user_ids, item_seqs, item_ids, visual_feats):
        if self.use_seq_user:
            h            = self.seq_encoder(item_seqs)
            user_vec_mf  = self.proj_user_mf(h)
            user_vec_mlp = self.proj_user_mlp(h)
        else:
            user_vec_mf  = self.user_emb_mf(user_ids)
            user_vec_mlp = self.user_emb_mlp(user_ids)

        item_vec_mf  = self.item_emb_mf(item_ids)
        item_vec_mlp = self.item_emb_mlp(item_ids)
        if self.use_visual:
            item_vec_mf  = self.visual_fusion_mf( torch.cat([item_vec_mf,  visual_feats], dim=-1))
            item_vec_mlp = self.visual_fusion_mlp(torch.cat([item_vec_mlp, visual_feats], dim=-1))

        mf_out  = torch.mul(user_vec_mf, item_vec_mf)
        mlp_out = torch.cat([user_vec_mlp, item_vec_mlp], dim=-1)
        for layer in self.fc_layers:
            mlp_out = torch.relu(layer(mlp_out))

        return self.sigmoid(self.output_layer(torch.cat([mf_out, mlp_out], dim=-1)))


model = SeqNeuMF(config).to(DEVICE)
print(model)
```

Bước 8 - Định nghĩa các thang đo đánh giá
Khởi tạo công cụ đo lường hiệu suất `MetronAtK`. Dựa trên danh sách các target item và các negative item được dự đoán điểm số, lớp này sẽ tiến hành ranking và tính toán hai độ đo chuẩn trong hệ thống gợi ý: HR@10 và NDCG@10.

```python
class MetronAtK:
    def __init__(self, top_k=10):
        self._top_k    = top_k
        self._subjects = None

    @property
    def subjects(self):
        return self._subjects

    @subjects.setter
    def subjects(self, subjects):
        test_users, test_items, test_scores = subjects[0], subjects[1], subjects[2]
        neg_users,  neg_items,  neg_scores  = subjects[3], subjects[4], subjects[5]
        test = pd.DataFrame({'user': test_users, 'test_item': test_items, 'test_score': test_scores})
        full = pd.DataFrame({'user': neg_users + test_users,
                             'item': neg_items + test_items,
                             'score': neg_scores + test_scores})
        full = pd.merge(full, test, on='user', how='left')
        full['rank'] = full.groupby('user')['score'].rank(method='first', ascending=False)
        full.sort_values(['user', 'rank'], inplace=True)
        self._subjects = full

    def cal_hit_ratio(self):
        full, k = self._subjects, self._top_k
        topk = full[full['rank'] <= k]
        return len(topk[topk['test_item'] == topk['item']]) / full['user'].nunique()

    def cal_ndcg(self):
        full, k = self._subjects, self._top_k
        topk = full[full['rank'] <= k]
        hits = topk[topk['test_item'] == topk['item']].copy()
        hits['ndcg'] = hits['rank'].apply(lambda x: math.log(2) / math.log(1 + x))
        return hits['ndcg'].sum() / full['user'].nunique()

metron = MetronAtK(top_k=10)
```

Bước 9 - Thiết lập hàm mất mát và bộ tối ưu hóa
Tạo Optimizer theo thuật toán Adam dựa trên cấu hình ở phía trên. Loss Function được sử dụng là Binary Cross Entropy chẳng hạn như `BCELoss`, phù hợp đối với bài toán dự đoán xác suất tương tác.

```python
def use_optimizer(model, config):
    if config['optimizer'] == 'adam':
        return torch.optim.Adam(model.parameters(),
                                lr=config['adam_lr'],
                                weight_decay=config['l2_regularization'])
    raise ValueError(f"Unknown optimizer: {config['optimizer']}")

def save_checkpoint(model, path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    torch.save(model.state_dict(), path)

optimizer = use_optimizer(model, config)
criterion = nn.BCELoss()
```

Bước 10 - Xây dựng vòng lặp huấn luyện
Khối mã này chứa hai hàm `train_epoch` và `evaluate` cấu thành nên toàn bộ vòng lặp huấn luyện chính. Sau mỗi epoch, chương trình dùng tập test để tính toán độ đo, in ra tiến trình, và tự động lưu lại model checkpoint vào thư mục `checkpoints` nếu kết quả tốt hơn.

```python
def train_epoch(model, train_loader, optimizer, criterion, epoch_id, device):
    model.train()
    total_loss = 0
    for batch_id, batch in enumerate(tqdm(train_loader, desc=f"Epoch {epoch_id}")):
        users, seqs, items, ratings, visuals = [b.to(device) for b in batch]
        optimizer.zero_grad()
        preds = model(users, seqs, items, visuals)
        loss  = criterion(preds.view(-1), ratings)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
        if batch_id % 50 == 0:
            print(f"  [Epoch {epoch_id}] Batch {batch_id:4d}  loss={loss.item():.4f}")
    return total_loss / len(train_loader)


def evaluate(model, evaluate_data, epoch_id, metron, config, device):
    model.eval()
    test_users, test_seqs, test_items, test_visuals = evaluate_data[:4]
    neg_users,  neg_seqs,  neg_items,  neg_visuals  = evaluate_data[4:]

    test_scores, neg_scores = [], []
    bs = config['batch_size']

    with torch.no_grad():
        for s in range(0, len(test_users), bs):
            e = min(s + bs, len(test_users))
            bu   = test_users[s:e].to(device)
            bseq = test_seqs[s:e].to(device)
            bi   = test_items[s:e].to(device)
            bv   = test_visuals[s:e].to(device)
            test_scores.append(model(bu, bseq, bi, bv).cpu())

        for s in tqdm(range(0, len(neg_users), bs), desc="Eval negatives"):
            e = min(s + bs, len(neg_users))
            bu   = neg_users[s:e].to(device)
            bseq = neg_seqs[s:e].to(device)
            bi   = neg_items[s:e].to(device)
            bv   = neg_visuals[s:e].to(device)
            neg_scores.append(model(bu, bseq, bi, bv).cpu())

    test_scores = torch.cat(test_scores).view(-1).tolist()
    neg_scores  = torch.cat(neg_scores).view(-1).tolist()

    metron.subjects = [
        test_users.tolist(), test_items.tolist(), test_scores,
        neg_users.tolist(),  neg_items.tolist(),  neg_scores,
    ]
    hr   = metron.cal_hit_ratio()
    ndcg = metron.cal_ndcg()
    print(f"[Epoch {epoch_id}] HR@10 = {hr:.4f}  NDCG@10 = {ndcg:.4f}")
    return hr, ndcg


# ── Run training ──────────────────────────────────────────────────────────────
best_hr = 0.0
for epoch in range(config['num_epoch']):
    print(f"\n{'='*60}")
    print(f"Epoch {epoch} starts !")
    print('='*60)
    train_loader = sample_generator.instance_a_train_loader(
        config['num_negative'], config['batch_size'])
    avg_loss = train_epoch(model, train_loader, optimizer, criterion, epoch, DEVICE)
    print(f"\nAvg loss: {avg_loss:.4f}")
    hr, ndcg = evaluate(model, evaluate_data, epoch, metron, config, DEVICE)
    ckpt_path = config['model_dir'].format(config['alias'], epoch, hr, ndcg)
    save_checkpoint(model, ckpt_path)
    print(f"Checkpoint saved → {ckpt_path}")
    if hr > best_hr:
        best_hr = hr
        best_ckpt = ckpt_path

print(f"\nBest HR@10 = {best_hr:.4f}  →  {best_ckpt}")
```

<p align="center">
  <img src="../assets/loss.png" width="48%">
  <img src="../assets/hr_ndcg.png" width="48%">
  <br>
  Hình 2. Biểu đồ giá trị Loss và các độ đo HR@10, NDCG@10 trong quá trình huấn luyện.
</p>

Bước 11 - Suy luận và gợi ý cho người dùng cụ thể
Phần này thực hiện quá trình Inference cho một ID người dùng bất kỳ. Bằng cách tra cứu lịch sử, loại bỏ các video họ đã xem, mô hình sẽ tính toán điểm số cho các video còn lại và trả về top 10 video phù hợp để gợi ý.

```python
# ── CONFIG ────────────────────────────────────────────────────────────────────
TARGET_USER_ORIG = "12345"   # <-- thay bằng user ID gốc trong pairs.csv
TOP_K            = 10
CHECKPOINT_PATH  = best_ckpt  # hoặc đặt thủ công, ví dụ: "/content/checkpoints/seqneumf_...model"
MAXLEN           = config['maxlen']

# ── Load mappings ─────────────────────────────────────────────────────────────
interactions2 = pd.read_csv(os.path.join(DATA_DIR, 'pairs.csv'))
interactions2['user'] = interactions2['user'].astype(str)
interactions2['item'] = interactions2['item'].astype(str)

uid_map2 = interactions2[['user']].drop_duplicates().reset_index(drop=True)
uid_map2['userId'] = np.arange(len(uid_map2))
interactions2 = pd.merge(interactions2, uid_map2, on='user', how='left')

iid_map2 = interactions2[['item']].drop_duplicates().reset_index(drop=True)
iid_map2['itemId'] = np.arange(len(iid_map2))
interactions2 = pd.merge(interactions2, iid_map2, on='item', how='left')

# ── Find internal user id ─────────────────────────────────────────────────────
assert TARGET_USER_ORIG in uid_map2['user'].values, "User not found!"
internal_uid = int(uid_map2[uid_map2['user'] == TARGET_USER_ORIG]['userId'].iloc[0])

# ── Build history & sequence ──────────────────────────────────────────────────
user_hist = interactions2[interactions2['userId'] == internal_uid]['itemId'].tolist()
seq       = user_hist[-MAXLEN:]
padded    = [0] * (MAXLEN - len(seq)) + seq

all_items       = iid_map2['itemId'].tolist()
candidate_items = [i for i in all_items if i not in set(user_hist)]
print(f"User {TARGET_USER_ORIG}: seen {len(user_hist)} items, scoring {len(candidate_items)} candidates")

# ── Load visual embeddings ────────────────────────────────────────────────────
raw_v2   = torch.load(os.path.join(DATA_DIR, 'visual_embeddings.pt'), weights_only=False)
o2n2     = dict(zip(iid_map2['item'], iid_map2['itemId']))
vis_embs = {o2n2[k]: v for k, v in raw_v2.items() if k in o2n2}
default_v = torch.zeros(VISUAL_DIM)

# ── Load model ────────────────────────────────────────────────────────────────
infer_config = {**config,
                'num_users': uid_map2['userId'].nunique(),
                'num_items': iid_map2['itemId'].nunique(),
                'weight_init_gaussian': False,
                'dropout_rate': 0.0}
infer_model = SeqNeuMF(infer_config).to(DEVICE)
infer_model.load_state_dict(torch.load(CHECKPOINT_PATH, map_location=DEVICE, weights_only=True))
infer_model.eval()
print(f"Checkpoint loaded: {CHECKPOINT_PATH}")

# ── Score candidates ──────────────────────────────────────────────────────────
user_t  = torch.tensor([internal_uid] * len(candidate_items), dtype=torch.long).to(DEVICE)
item_t  = torch.tensor(candidate_items,                        dtype=torch.long).to(DEVICE)
seq_t   = torch.tensor([padded]       * len(candidate_items), dtype=torch.long).to(DEVICE)
vis_t   = torch.stack([vis_embs.get(i, default_v) for i in candidate_items]).to(DEVICE)

with torch.no_grad():
    scores = infer_model(user_t, seq_t, item_t, vis_t).squeeze().cpu().numpy()

top_k_pairs = sorted(zip(candidate_items, scores), key=lambda x: x[1], reverse=True)[:TOP_K]

# ── Lookup titles ─────────────────────────────────────────────────────────────
try:
    titles_df = pd.read_csv(os.path.join(DATA_DIR, 'titles.csv'))
    titles_df['item'] = titles_df['item'].astype(str)
    title_col = 'title' if 'title' in titles_df.columns else titles_df.columns[-1]
    orig_to_title = dict(zip(titles_df['item'], titles_df[title_col]))
    item_to_orig  = dict(zip(iid_map2['itemId'], iid_map2['item']))
    get_title = lambda iid: orig_to_title.get(item_to_orig.get(iid, ''), f"Video {iid}")
except FileNotFoundError:
    item_to_orig = dict(zip(iid_map2['itemId'], iid_map2['item']))
    get_title = lambda iid: f"Video {item_to_orig.get(iid, iid)}"

# ── Print results ─────────────────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  TOP {TOP_K} GỢI Ý CHO USER: {TARGET_USER_ORIG}")
print('='*55)
for rank, (iid, score) in enumerate(top_k_pairs, 1):
    print(f"  {rank:2d}. [Score: {score:.4f}]  {get_title(iid)}")
print('='*55)
```

<p align="center">
  <img src="../assets/results.png" width="80%">
  <br>
  Hình 3. Kết quả gợi ý video cho user có ID = 1001. Hình ảnh minh họa kết quả đầu ra của mô hình khi được triển khai thành giao diện ứng dụng web thực tế. Chi tiết mã nguồn tại <a href="https://github.com/T-Sunm/MM-ShortVideo-Rec">T-Sunm/MM-ShortVideo-Rec</a>.
</p>
