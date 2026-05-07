import torch
import random
import pandas as pd
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
    """
    Construct dataset for NCF using pre-processed samples from Lakehouse Gold Layer.
    Binarization, Splitting, and Sequence Building are already handled by the dbt pipeline.
    """

    def __init__(self, gold_df: pd.DataFrame, visual_embeddings: dict, maxlen: int = 50):
        self.visual_embeddings = visual_embeddings
        self.maxlen            = maxlen
        self.item_pool         = set(gold_df['item'].unique())

        # Use pre-defined splits from dbt
        self.train_ratings = gold_df[gold_df['split'] == 'train'].reset_index(drop=True)
        # For testing, we typically use the last interaction in the test split
        self.test_ratings  = (
            gold_df[gold_df['split'] == 'test']
            .sort_values('timestamp')
            .groupby('user_id').last()
            .reset_index()
        )

        # Negative sampling (still needed in Python)
        self.neg_items = gold_df.groupby('user_id')['item'].apply(set).to_dict()
        self.negatives = self._sample_negative_evaluation(self.test_ratings)

    def _sample_negative_evaluation(self, test_df):
        """Sample 99 negatives for each user for evaluation."""
        neg_samples = []
        for row in test_df.itertuples():
            uid = row.user_id
            interacted = self.neg_items.get(uid, set())
            neg_pool = list(self.item_pool - interacted)
            neg_samples.append(random.sample(neg_pool, 99))
        
        test_df['negative_samples'] = neg_samples
        return test_df[['user_id', 'negative_samples']]

    def _pad(self, seq):
        """Left-pad sequence to maxlen."""
        seq = list(seq)
        seq = seq[-self.maxlen:]
        return [0] * (self.maxlen - len(seq)) + seq

    def instance_a_train_loader(self, num_negatives, batch_size):
        users, seqs, items, ratings = [], [], [], []
        for row in self.train_ratings.itertuples():
            uid, iid = int(row.user_id), int(row.item)
            seq = self._pad(row.s_item)
            
            # Positive sample
            users.append(uid); seqs.append(seq); items.append(iid); ratings.append(1.0)
            
            # Negative samples
            neg_pool = list(self.item_pool - self.neg_items.get(uid, set()))
            for neg in random.sample(neg_pool, num_negatives):
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
        test = pd.merge(self.test_ratings, self.negatives, on='user_id')
        test_users, test_seqs, test_items, neg_users, neg_seqs, neg_items = [], [], [], [], [], []
        
        for row in test.itertuples():
            uid, iid = int(row.user_id), int(row.item)
            seq = self._pad(row.s_item)
            
            test_users.append(uid); test_seqs.append(seq); test_items.append(iid)
            for neg in row.negative_samples:
                neg_users.append(uid); neg_seqs.append(seq); neg_items.append(int(neg))
                
        test_items_t = torch.LongTensor(test_items)
        neg_items_t  = torch.LongTensor(neg_items)
        return [
            torch.LongTensor(test_users), torch.LongTensor(test_seqs), test_items_t, VisualLookup(test_items_t, self.visual_embeddings),
            torch.LongTensor(neg_users),  torch.LongTensor(neg_seqs),  neg_items_t,  VisualLookup(neg_items_t,  self.visual_embeddings),
        ]
