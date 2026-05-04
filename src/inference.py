import os
import argparse
import torch
import pandas as pd
import numpy as np

from neumf import NeuMF
from seqneumf import SeqNeuMF

def parse_args():
    parser = argparse.ArgumentParser(description="Inference script to get video recommendations for a specific user.")
    parser.add_argument("--data_dir",       type=str, default="data/microlens-5k",
                        help="Path to dataset directory")
    parser.add_argument("--checkpoint",     type=str, required=True,
                        help="Path to the trained model checkpoint (.model file)")
    parser.add_argument("--user_id",        type=str, required=True,
                        help="Original user ID from pairs.csv to recommend videos for")
    parser.add_argument("--top_k",          type=int, default=10,
                        help="Number of top items to recommend")
    parser.add_argument("--use_cuda",       action="store_true", default=False)
    parser.add_argument("--no_visual",      action="store_true", default=False,
                        help="Disable visual features (ablation study)")
                        
    # SeqNeuMF arguments
    parser.add_argument("--use_seq_user",   action="store_true", default=False,
                        help="Use sequence-based user representation (SeqNeuMF)")
    return parser.parse_args()

def main():
    args = parse_args()

    # 1. Tải và build mapping (để khớp với index lúc train model)
    print(f"Loading data from {args.data_dir}...")
    interactions = pd.read_csv(os.path.join(args.data_dir, 'pairs.csv'))
    interactions['user'] = interactions['user'].astype(str)
    interactions['item'] = interactions['item'].astype(str)

    # Reindex userId
    user_id_map = interactions[['user']].drop_duplicates().reset_index(drop=True)
    user_id_map['userId'] = np.arange(len(user_id_map))
    interactions = pd.merge(interactions, user_id_map, on=['user'], how='left')

    # Reindex itemId
    item_id_map = interactions[['item']].drop_duplicates().reset_index(drop=True)
    item_id_map['itemId'] = np.arange(len(item_id_map))
    interactions = pd.merge(interactions, item_id_map, on=['item'], how='left')

    # 2. Tìm userId mapping
    if args.user_id not in user_id_map['user'].values:
        print(f"Error: User '{args.user_id}' not found in the dataset.")
        return

    internal_user_id = user_id_map[user_id_map['user'] == args.user_id]['userId'].iloc[0]
    
    # 3. Lấy ra những items mà user này đã tương tác (theo thứ tự timestamp nếu dataframe đã sort)
    user_history = interactions[interactions['userId'] == internal_user_id]['itemId'].tolist()
    
    # Chuẩn bị Sequence cho model SeqNeuMF
    if args.use_seq_user:
        maxlen = 50 # Mặc định như trong file train.py
        seq = user_history[-maxlen:]
        padded_seq = [0] * (maxlen - len(seq)) + seq
        print(f"Built user sequence length {len(seq)} (padded to {maxlen})")
    
    # Những items có thể đem ra để recommend là những items có trong item_pool mà user chưa xem
    all_item_ids = item_id_map['itemId'].tolist()
    candidate_items = [i for i in all_item_ids if i not in user_history]
    
    print(f"User {args.user_id} has seen {len(user_history)} videos.")
    print(f"Scoring {len(candidate_items)} new candidate videos...")

    # 4. Load Visual Embeddings
    if args.no_visual:
        visual_embeddings = {}
    else:
        raw_visual   = torch.load(os.path.join(args.data_dir, 'visual_embeddings.pt'), weights_only=False)
        orig_to_new  = dict(zip(item_id_map['item'], item_id_map['itemId']))
        visual_embeddings = {orig_to_new[k]: v for k, v in raw_visual.items() if k in orig_to_new}

    # 5. Build Model Architecture
    config = {
        'num_users': user_id_map['userId'].nunique(),
        'num_items': item_id_map['itemId'].nunique(),
        'latent_dim_mf': 8,
        'latent_dim_mlp': 8,
        'layers': [16, 64, 32, 16, 8],
        'visual_dim': 0 if args.no_visual else 768,
        'weight_init_gaussian': False # Not initing because we are loading checkpoint
    }

    if args.use_seq_user:
        config.update({
            'use_seq_user': True,
            'maxlen': 50,
            'seq_hidden_units': 50,
            'num_heads': 1,
            'num_blocks': 2,
            'dropout_rate': 0.0 # eval mode
        })
        model = SeqNeuMF(config)
    else:
        model = NeuMF(config)
        
    print(f"Loading checkpoint from: {args.checkpoint}")
    device = torch.device('cuda' if args.use_cuda and torch.cuda.is_available() else 'cpu')
    model.load_state_dict(torch.load(args.checkpoint, map_location=device, weights_only=True))
    model.to(device)
    model.eval()

    # 6. Predict scores
    user_tensor = torch.tensor([internal_user_id] * len(candidate_items), dtype=torch.long).to(device)
    item_tensor = torch.tensor(candidate_items, dtype=torch.long).to(device)
    
    visual_tensors = []
    default_visual = torch.zeros(config.get('visual_dim', 768)) if not args.no_visual else torch.zeros(0)
    for i in candidate_items:
        if not args.no_visual:
            visual_tensors.append(visual_embeddings.get(i, default_visual))
        else:
            visual_tensors.append(default_visual)
            
    visual_tensor_batch = torch.stack(visual_tensors).to(device) if len(visual_tensors) > 0 and not args.no_visual else torch.empty((len(candidate_items), 0)).to(device)

    if args.use_seq_user:
        seq_tensor = torch.tensor([padded_seq] * len(candidate_items), dtype=torch.long).to(device)

    with torch.no_grad():
        if args.use_seq_user:
            scores = model(user_tensor, seq_tensor, item_tensor, visual_tensor_batch)
        else:
            scores = model(user_tensor, item_tensor, visual_tensor_batch)
        scores = scores.squeeze().cpu().numpy()

    # 7. Sắp xếp điểm số
    candidate_scores = list(zip(candidate_items, scores))
    candidate_scores.sort(key=lambda x: x[1], reverse=True)
    top_k_items = candidate_scores[:args.top_k]

    # 8. Lookup Titles
    try:
        titles_df = pd.read_csv(os.path.join(args.data_dir, 'titles.csv'))
        titles_df['item'] = titles_df['item'].astype(str)
        # Create dict: internal_itemId -> title
        item_to_orig = dict(zip(item_id_map['itemId'], item_id_map['item']))
        orig_to_title = dict(zip(titles_df['item'], titles_df.get('title', titles_df.get('text', 'Unknown Title'))))
        
        internal_item_to_title = {iid: orig_to_title.get(orig, f"Video {orig}") for iid, orig in item_to_orig.items()}
    except FileNotFoundError:
        # Fallback if titles.csv is missing
        print("Warning: titles.csv not found, displaying original item IDs only.")
        item_to_orig = dict(zip(item_id_map['itemId'], item_id_map['item']))
        internal_item_to_title = {iid: f"Video {orig}" for iid, orig in item_to_orig.items()}

    # 9. Output Gợi ý
    print("\n" + "="*50)
    print(f"🌟 TOP {args.top_k} GỢI Ý CHO USER: {args.user_id} 🌟")
    print("="*50)
    for rank, (iid, score) in enumerate(top_k_items, 1):
        title = internal_item_to_title.get(iid, "Unknown")
        print(f"{rank:2d}. [Score: {score:.4f}] {title}")
    print("="*50)

if __name__ == '__main__':
    main()
