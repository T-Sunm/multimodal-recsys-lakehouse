import os
import argparse
import torch
import pandas as pd
import numpy as np
from gmf import GMFEngine
from mlp import MLPEngine
from neumf import NeuMFEngine
from seqneumf import SeqNeuMF
from data_utils import SampleGenerator
from engine import Engine
from utils import use_cuda
from trino_service import fetch_training_samples, fetch_visual_embeddings

class SeqNeuMFEngine(Engine):
    """Engine for training & evaluating SeqNeuMF model"""
    def __init__(self, config):
        self.model = SeqNeuMF(config)
        if config['use_cuda'] is True:
            use_cuda(True, config['device_id'])
            self.model.cuda()
        super(SeqNeuMFEngine, self).__init__(config)
        print(self.model)


def parse_args():
    parser = argparse.ArgumentParser(description="Train NeuMF with Lakehouse Gold Features")
    parser.add_argument("--checkpoint_dir", type=str, default="checkpoints")
    parser.add_argument("--num_epoch",      type=int, default=1)
    parser.add_argument("--batch_size",     type=int, default=1024)
    parser.add_argument("--use_cuda",       action="store_true", default=False)
    parser.add_argument("--no_visual",      action="store_true", default=False)
    parser.add_argument("--use_seq_user",   action="store_true", default=False)
    parser.add_argument("--model",          type=str, default="neumf", choices=["gmf", "mlp", "neumf", "seqneumf"])
    return parser.parse_args()

gmf_config = {'alias': 'gmf_factor8neg4-implict',
              'num_epoch': 200, 'batch_size': 1024, 'optimizer': 'adam', 'adam_lr': 1e-3,
              'num_users': 6040, 'num_items': 3706, 'latent_dim': 8, 'num_negative': 4,
              'l2_regularization': 0, 'weight_init_gaussian': True, 'use_cuda': False,
              'use_bachify_eval': False, 'device_id': 0,
              'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'}

mlp_config = {'alias': 'mlp_factor8neg4_bz256_166432168_pretrain_reg_0.0000001',
              'num_epoch': 200, 'batch_size': 256, 'optimizer': 'adam', 'adam_lr': 1e-3,
              'num_users': 6040, 'num_items': 3706, 'latent_dim': 8, 'num_negative': 4,
              'layers': [16, 64, 32, 16, 8], 'l2_regularization': 0.0000001, 'weight_init_gaussian': True,
              'use_cuda': False, 'use_bachify_eval': False, 'device_id': 0, 'pretrain': False,
              'pretrain_mf': 'checkpoints/gmf_factor8neg4_Epoch100_HR0.6391_NDCG0.2852.model',
              'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'}

neumf_config = {'alias': 'neumf_factor8neg4',
                'num_epoch': 1, 'batch_size': 1024, 'optimizer': 'adam', 'adam_lr': 1e-3,
                'num_users': 6040, 'num_items': 3706, 'latent_dim_mf': 8, 'latent_dim_mlp': 8, 'num_negative': 4,
                'layers': [16, 64, 32, 16, 8], 'l2_regularization': 0.0000001, 'weight_init_gaussian': True,
                'use_cuda': True, 'use_bachify_eval': True, 'device_id': 0, 'pretrain': False,
                'pretrain_mf': 'checkpoints/gmf_factor8neg4_Epoch100_HR0.6391_NDCG0.2852.model',
                'pretrain_mlp': 'checkpoints/mlp_factor8neg4_Epoch100_HR0.5606_NDCG0.2463.model',
                'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model', 'visual_dim': 768}

seqneumf_config = {'alias': 'seqneumf_factor8neg4',
                   'num_epoch': 1, 'batch_size': 1024, 'optimizer': 'adam', 'adam_lr': 1e-3,
                   'num_users': 6040, 'num_items': 3706, 'latent_dim_mf': 8, 'latent_dim_mlp': 8, 'num_negative': 4,
                   'layers': [16, 64, 32, 16, 8], 'l2_regularization': 0.0000001, 'weight_init_gaussian': True,
                   'use_cuda': True, 'use_bachify_eval': True, 'device_id': 0, 'visual_dim': 768,
                   'maxlen': 50, 'seq_hidden_units': 50, 'num_heads': 1, 'num_blocks': 2, 'dropout_rate': 0.2,
                   'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'}

args = parse_args()

# Load Data from Gold Layer
gold_df = fetch_training_samples()

# Reindex IDs (Contiguous 0-based for Embedding layers)
# We collect all unique users/items from interactions and sequences to ensure consistency
all_users = sorted(gold_df['user_id'].unique())
all_items = sorted(set(gold_df['item'].unique()) | {iid for seq in gold_df['s_item'] for iid in seq})

user_map = {uid: i for i, uid in enumerate(all_users)}
item_map = {iid: i for i, iid in enumerate(all_items)}

gold_df['user_id'] = gold_df['user_id'].map(user_map)
gold_df['item']    = gold_df['item'].map(item_map)
gold_df['s_item']  = gold_df['s_item'].apply(lambda seq: [item_map.get(x, 0) for x in seq])

# Load visual embeddings
if args.no_visual:
    visual_embeddings = {}
else:
    visual_embeddings = fetch_visual_embeddings(item_map)

# Specify the exact model
if args.model == 'gmf':
    config = gmf_config
elif args.model == 'mlp':
    config = mlp_config
elif args.use_seq_user or args.model == 'seqneumf':
    config = seqneumf_config
    config['use_seq_user'] = True
else:
    config = neumf_config

# Update config with dynamic data stats
config.update({
    'num_users':  len(all_users),
    'num_items':  len(all_items),
    'num_epoch':  args.num_epoch,
    'batch_size': args.batch_size,
    'use_cuda':   args.use_cuda,
    'visual_dim': 0 if args.no_visual else 768,
    'model_dir':  os.path.join(args.checkpoint_dir, '{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model')
})

# DataLoader for training
maxlen = config.get('maxlen', 50)
sample_generator = SampleGenerator(gold_df=gold_df, visual_embeddings=visual_embeddings, maxlen=maxlen)
evaluate_data = sample_generator.evaluate_data

if args.model == 'gmf':
    engine = GMFEngine(config)
elif args.model == 'mlp':
    engine = MLPEngine(config)
elif args.use_seq_user or args.model == 'seqneumf':
    engine = SeqNeuMFEngine(config)
else:
    engine = NeuMFEngine(config)

for epoch in range(config['num_epoch']):
    print(f'Epoch {epoch} starts !')
    print('-' * 80)
    train_loader = sample_generator.instance_a_train_loader(config['num_negative'], config['batch_size'])
    engine.train_an_epoch(train_loader, epoch_id=epoch)
    hit_ratio, ndcg = engine.evaluate(evaluate_data, epoch_id=epoch)
    engine.save(config['alias'], epoch, hit_ratio, ndcg)
