import os
import torch
from datetime import datetime
from torch.autograd import Variable
from tqdm import tqdm
from tensorboardX import SummaryWriter
from utils import save_checkpoint, use_optimizer
from metrics import MetronAtK


class Engine(object):
    """Meta Engine for training & evaluating NCF model

    Note: Subclass should implement self.model !
    """

    def __init__(self, config):
        self.config = config  # model configuration
        self._metron = MetronAtK(top_k=10)
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self._writer = SummaryWriter(log_dir='runs/{}_{}'.format(config['alias'], run_id))  # tensorboard writer
        self._writer.add_text('config', str(config), 0)
        self.opt = use_optimizer(self.model, config)
        # explicit feedback
        # self.crit = torch.nn.MSELoss()
        # implicit feedback
        self.crit = torch.nn.BCELoss()
        self._last_ckpt = None

    def train_single_batch(self, users, seqs, items, ratings, visuals):
        assert hasattr(self, 'model'), 'Please specify the exact model !'
        if self.config['use_cuda'] is True:
            users, seqs, items, ratings, visuals = users.cuda(), seqs.cuda(), items.cuda(), ratings.cuda(), visuals.cuda()
        self.opt.zero_grad()
        
        # Sửa để truyền thêm seqs vào model
        # Các model không dùng seq (như NeuMF gốc) cần được sửa để có thể nhận param seqs hoặc dùng **kwargs
        try:
            ratings_pred = self.model(users, seqs, items, visuals)
        except TypeError:
            ratings_pred = self.model(users, items, visuals) # Fallback cho model cũ
            
        loss = self.crit(ratings_pred.view(-1), ratings)
        loss.backward()
        self.opt.step()
        return loss.item()

    def train_an_epoch(self, train_loader, epoch_id):
        assert hasattr(self, 'model'), 'Please specify the exact model !'
        self.model.train()
        total_loss = 0
        for batch_id, batch in enumerate(train_loader):
            assert isinstance(batch[0], torch.LongTensor)
            user, seq, item, rating, visual = batch[0], batch[1], batch[2], batch[3].float(), batch[4]
            loss = self.train_single_batch(user, seq, item, rating, visual)
            print('[Training Epoch {}] Batch {}, Loss {}'.format(epoch_id, batch_id, loss))
            total_loss += loss
        self._writer.add_scalar('model/loss', total_loss / len(train_loader), epoch_id)

    def evaluate(self, evaluate_data, epoch_id):
        assert hasattr(self, 'model'), 'Please specify the exact model !'
        self.model.eval()
        with torch.no_grad():
            test_users, test_seqs, test_items, test_visuals = evaluate_data[0], evaluate_data[1], evaluate_data[2], evaluate_data[3]
            negative_users, negative_seqs, negative_items, negative_visuals = evaluate_data[4], evaluate_data[5], evaluate_data[6], evaluate_data[7]

            if self.config['use_bachify_eval'] == False:
                try:
                    test_scores     = self.model(test_users, test_seqs, test_items, test_visuals)
                    negative_scores = self.model(negative_users, negative_seqs, negative_items, negative_visuals)
                except TypeError:
                    test_scores     = self.model(test_users, test_items, test_visuals)
                    negative_scores = self.model(negative_users, negative_items, negative_visuals)
            else:
                test_scores     = []
                negative_scores = []
                bs = self.config['batch_size']
                for start_idx in range(0, len(test_users), bs):
                    end_idx = min(start_idx + bs, len(test_users))
                    bu, bseq, bi, bv = test_users[start_idx:end_idx], test_seqs[start_idx:end_idx], test_items[start_idx:end_idx], test_visuals[start_idx:end_idx]
                    if self.config['use_cuda']:
                        bu, bseq, bi, bv = bu.cuda(), bseq.cuda(), bi.cuda(), bv.cuda()
                    try:
                        test_scores.append(self.model(bu, bseq, bi, bv).cpu())
                    except TypeError:
                        test_scores.append(self.model(bu, bi, bv).cpu())

                for start_idx in tqdm(range(0, len(negative_users), bs), desc="Eval Negatives"):
                    end_idx = min(start_idx + bs, len(negative_users))
                    bu, bseq, bi, bv = negative_users[start_idx:end_idx], negative_seqs[start_idx:end_idx], negative_items[start_idx:end_idx], negative_visuals[start_idx:end_idx]
                    if self.config['use_cuda']:
                        bu, bseq, bi, bv = bu.cuda(), bseq.cuda(), bi.cuda(), bv.cuda()
                    try:
                        negative_scores.append(self.model(bu, bseq, bi, bv).cpu())
                    except TypeError:
                        negative_scores.append(self.model(bu, bi, bv).cpu())

                test_scores     = torch.concatenate(test_scores, dim=0)
                negative_scores = torch.concatenate(negative_scores, dim=0)

                self._metron.subjects = [test_users.data.view(-1).tolist(),
                                     test_items.data.view(-1).tolist(),
                                     test_scores.data.view(-1).tolist(),
                                     negative_users.data.view(-1).tolist(),
                                     negative_items.data.view(-1).tolist(),
                                     negative_scores.data.view(-1).tolist()]
        hit_ratio, ndcg = self._metron.cal_hit_ratio(), self._metron.cal_ndcg()
        self._writer.add_scalar('performance/HR', hit_ratio, epoch_id)
        self._writer.add_scalar('performance/NDCG', ndcg, epoch_id)
        print('[Evluating Epoch {}] HR = {:.4f}, NDCG = {:.4f}'.format(epoch_id, hit_ratio, ndcg))
        return hit_ratio, ndcg


    def save(self, alias, epoch_id, hit_ratio, ndcg):
        assert hasattr(self, 'model'), 'Please specify the exact model !'
        model_dir = self.config['model_dir'].format(alias, epoch_id, hit_ratio, ndcg)
        save_checkpoint(self.model, model_dir)
        
        # Xoá checkpoint cũ nếu có
        if self._last_ckpt is not None and os.path.exists(self._last_ckpt):
            try:
                os.remove(self._last_ckpt)
            except OSError:
                pass
        self._last_ckpt = model_dir