import torch
import random
import pandas as pd
from copy import deepcopy
from torch.utils.data import DataLoader, Dataset

random.seed(0)

VISUAL_DIM = 768


class UserItemRatingDataset(Dataset):
    """<user, seq, item, rating, visual> dataset with lazy visual lookup."""

    def __init__(self, user_tensor, seq_tensor, item_tensor, target_tensor, visual_embeddings):
        self.user_tensor      = user_tensor
        self.seq_tensor       = seq_tensor
        self.item_tensor      = item_tensor
        self.target_tensor    = target_tensor
        self.visual_embeddings = visual_embeddings

    def __getitem__(self, index):
        item_id = self.item_tensor[index].item()
        visual  = self.visual_embeddings.get(item_id, torch.zeros(VISUAL_DIM))
        return self.user_tensor[index], self.seq_tensor[index], self.item_tensor[index], self.target_tensor[index], visual

    def __len__(self):
        return self.user_tensor.size(0)


class VisualLookup:
    """Lazy visual embedding lookup indexed by item tensor"""

    def __init__(self, items, visual_embeddings):
        self.items             = items
        self.visual_embeddings = visual_embeddings

    def __getitem__(self, idx):
        ids = self.items[idx]
        if ids.dim() == 0:
            return self.visual_embeddings.get(ids.item(), torch.zeros(VISUAL_DIM))
        return torch.stack([self.visual_embeddings.get(i.item(), torch.zeros(VISUAL_DIM)) for i in ids])

    def __len__(self):
        return len(self.items)


class SampleGenerator:
    """Construct dataset for NCF with sequence support."""

    def __init__(self, ratings: pd.DataFrame, visual_embeddings: dict, maxlen: int = 50):
        assert {'userId', 'itemId', 'rating', 'timestamp'}.issubset(ratings.columns)

        self.ratings           = ratings
        self.visual_embeddings = visual_embeddings
        self.maxlen            = maxlen
        self.user_pool         = set(ratings['userId'].unique())
        self.item_pool         = set(ratings['itemId'].unique())
        self.negatives         = self._sample_negative(ratings)
        self.train_ratings, self.test_ratings = self._split_loo(self._binarize(ratings))
        
        # Build user history sorted by timestamp for sequence generation
        sorted_ratings = ratings.sort_values(['userId', 'timestamp'], ascending=[True, True])
        self.user_history = sorted_ratings.groupby('userId')['itemId'].apply(list).to_dict()

    def _binarize(self, ratings):
        ratings = ratings.copy()
        ratings.loc[ratings['rating'] > 0, 'rating'] = 1.0
        return ratings

    def _split_loo(self, ratings):
        """Leave-one-out train/test split by timestamp."""
        ratings['rank_latest'] = ratings.groupby('userId')['timestamp'].rank(method='first', ascending=False)
        test  = ratings[ratings['rank_latest'] == 1]
        train = ratings[ratings['rank_latest'] >  1]
        assert train['userId'].nunique() == test['userId'].nunique()
        return train[['userId', 'itemId', 'rating']], test[['userId', 'itemId', 'rating']]

    def _sample_negative(self, ratings):
        interact_status = (
            ratings.groupby('userId')['itemId']
            .apply(set).reset_index()
            .rename(columns={'itemId': 'interacted_items'})
        )
        interact_status['negative_items']   = interact_status['interacted_items'].apply(lambda x: self.item_pool - x)
        interact_status['negative_samples'] = interact_status['negative_items'].apply(lambda x: random.sample(list(x), 99))
        # Store as dict for O(1) lookup — avoids storing large sets inside DataFrame and costly merge
        self._neg_items = dict(zip(interact_status['userId'], interact_status['negative_items']))
        return interact_status[['userId', 'negative_samples']]

    def _get_seq(self, uid, target_iid):
        """Get sequence of items viewed before target_iid (padded with 0s at the left)."""
        hist = self.user_history.get(uid, [])
        try:
            idx = hist.index(target_iid)
            seq = hist[:idx] # Items before the target item
        except ValueError:
            seq = hist       # Fallback if item not found (e.g. negative item, or test item)
            
        # Pad or truncate
        seq = seq[-self.maxlen:]
        padded_seq = [0] * (self.maxlen - len(seq)) + seq
        return padded_seq

    def instance_a_train_loader(self, num_negatives, batch_size):
        users, seqs, items, ratings = [], [], [], []
        for row in self.train_ratings.itertuples():
            uid, iid = int(row.userId), int(row.itemId)
            seq = self._get_seq(uid, iid)
            
            users.append(uid); seqs.append(seq); items.append(iid); ratings.append(float(row.rating))
            for neg in random.sample(list(self._neg_items[uid]), num_negatives):
                users.append(uid); seqs.append(seq); items.append(int(neg)); ratings.append(0.0)
        
        dataset = UserItemRatingDataset(
            user_tensor=torch.LongTensor(users),
            seq_tensor=torch.LongTensor(seqs),
            item_tensor=torch.LongTensor(items),
            target_tensor=torch.FloatTensor(ratings),
            visual_embeddings=self.visual_embeddings,
        )
        return DataLoader(dataset, batch_size=batch_size, shuffle=True)

    @property
    def evaluate_data(self):
        test = pd.merge(self.test_ratings, self.negatives[['userId', 'negative_samples']], on='userId')
        test_users, test_seqs, test_items, neg_users, neg_seqs, neg_items = [], [], [], [], [], []
        for row in test.itertuples():
            uid, iid = int(row.userId), int(row.itemId)
            # Dùng tất cả lịch sử (không bao gồm test_item)
            hist = self.user_history.get(uid, [])
            try:
                idx = hist.index(iid)
                seq = hist[:idx]
            except:
                seq = hist[:-1] # test item usually at the end
            seq = seq[-self.maxlen:]
            padded_seq = [0] * (self.maxlen - len(seq)) + seq
            
            test_users.append(uid)
            test_seqs.append(padded_seq)
            test_items.append(iid)
            for neg in row.negative_samples:
                neg_users.append(uid)
                neg_seqs.append(padded_seq)
                neg_items.append(int(neg))
                
        test_items_t = torch.LongTensor(test_items)
        neg_items_t  = torch.LongTensor(neg_items)
        return [
            torch.LongTensor(test_users), torch.LongTensor(test_seqs), test_items_t, VisualLookup(test_items_t, self.visual_embeddings),
            torch.LongTensor(neg_users),  torch.LongTensor(neg_seqs),  neg_items_t,  VisualLookup(neg_items_t,  self.visual_embeddings),
        ]
