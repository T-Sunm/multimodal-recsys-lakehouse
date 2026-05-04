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
    parser = argparse.ArgumentParser(description="Train NeuMF with visual features")
    parser.add_argument("--data_dir",       type=str, default="data/microlens-5k",
                        help="Path to dataset directory")
    parser.add_argument("--checkpoint_dir", type=str, default="checkpoints",
                        help="Directory to save model checkpoints")
    parser.add_argument("--num_epoch",      type=int, default=1)
    parser.add_argument("--batch_size",     type=int, default=1024)
    parser.add_argument("--use_cuda",       action="store_true", default=False)
    parser.add_argument("--no_visual",      action="store_true", default=False,
                        help="Disable visual features (ablation study)")
    
    # SeqNeuMF arguments
    parser.add_argument("--use_seq_user",   action="store_true", default=False,
                        help="Use sequence-based user representation (SeqNeuMF)")
    return parser.parse_args()

gmf_config = {'alias': 'gmf_factor8neg4-implict',
              'num_epoch': 200,
              'batch_size': 1024,
              # 'optimizer': 'sgd',
              # 'sgd_lr': 1e-3,
              # 'sgd_momentum': 0.9,
              # 'optimizer': 'rmsprop',
              # 'rmsprop_lr': 1e-3,
              # 'rmsprop_alpha': 0.99,
              # 'rmsprop_momentum': 0,
              'optimizer': 'adam',
              'adam_lr': 1e-3,
              'num_users': 6040,
              'num_items': 3706,
              'latent_dim': 8,
              'num_negative': 4,
              'l2_regularization': 0,  # 0.01
              'weight_init_gaussian': True,
              'use_cuda': False,
              'use_bachify_eval': False,
              'device_id': 0,
              'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'}

mlp_config = {'alias': 'mlp_factor8neg4_bz256_166432168_pretrain_reg_0.0000001',
              'num_epoch': 200,
              'batch_size': 256,  # 1024,
              'optimizer': 'adam',
              'adam_lr': 1e-3,
              'num_users': 6040,
              'num_items': 3706,
              'latent_dim': 8,
              'num_negative': 4,
              'layers': [16, 64, 32, 16, 8],  # layers[0] is the concat of latent user vector & latent item vector
              'l2_regularization': 0.0000001,  # MLP model is sensitive to hyper params
              'weight_init_gaussian': True,
              'use_cuda': False,
              'use_bachify_eval': False,
              'device_id': 0,
              'pretrain': False,
              'pretrain_mf': 'checkpoints/{}'.format('gmf_factor8neg4_Epoch100_HR0.6391_NDCG0.2852.model'),
              'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'}

neumf_config = {'alias': 'neumf_factor8neg4',
                'num_epoch': 1,
                'batch_size': 1024,
                'optimizer': 'adam',
                'adam_lr': 1e-3,
                'num_users': 6040,
                'num_items': 3706,
                'latent_dim_mf': 8,
                'latent_dim_mlp': 8,
                'num_negative': 4,
                'layers': [16, 64, 32, 16, 8],  # layers[0] is the concat of latent user vector & latent item vector
                'l2_regularization': 0.0000001,
                'weight_init_gaussian': True,
                'use_cuda': True,
                'use_bachify_eval': True,
                'device_id': 0,
                'pretrain': False,
                'pretrain_mf': 'checkpoints/{}'.format('gmf_factor8neg4_Epoch100_HR0.6391_NDCG0.2852.model'),
                'pretrain_mlp': 'checkpoints/{}'.format('mlp_factor8neg4_Epoch100_HR0.5606_NDCG0.2463.model'),
                'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model',
                'visual_dim': 768,
                }

seqneumf_config = {'alias': 'seqneumf_factor8neg4',
                   'num_epoch': 1,
                   'batch_size': 1024,
                   'optimizer': 'adam',
                   'adam_lr': 1e-3,
                   'num_users': 6040,
                   'num_items': 3706,
                   'latent_dim_mf': 8,
                   'latent_dim_mlp': 8,
                   'num_negative': 4,
                   'layers': [16, 64, 32, 16, 8],
                   'l2_regularization': 0.0000001,
                   'weight_init_gaussian': True,
                   'use_cuda': True,
                   'use_bachify_eval': True,
                   'device_id': 0,
                   'visual_dim': 768,
                   'maxlen': 50,
                   'seq_hidden_units': 50,
                   'num_heads': 1,
                   'num_blocks': 2,
                   'dropout_rate': 0.2,
                   'model_dir': 'checkpoints/{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model'}

args = parse_args()

# Load Data
interactions = pd.read_csv(os.path.join(args.data_dir, 'pairs.csv'))

# Tiền xử lý MicroLens: user, item, timestamp
interactions['rating'] = 1.0  # Implicit feedback

# Reindex userId
user_id_map = interactions[['user']].drop_duplicates().reset_index(drop=True)
user_id_map['userId'] = np.arange(len(user_id_map))
interactions = pd.merge(interactions, user_id_map, on=['user'], how='left')

# Reindex itemId và giữ lại mapping item_id_original → itemId
item_id_map = interactions[['item']].drop_duplicates().reset_index(drop=True)
item_id_map['itemId'] = np.arange(len(item_id_map))
interactions = pd.merge(interactions, item_id_map, on=['item'], how='left')

ml1m_rating = interactions[['userId', 'itemId', 'rating', 'timestamp']]

print('Range of userId is [{}, {}]'.format(ml1m_rating.userId.min(), ml1m_rating.userId.max()))
print('Range of itemId is [{}, {}]'.format(ml1m_rating.itemId.min(), ml1m_rating.itemId.max()))

# Load visual embeddings
if args.no_visual:
    visual_embeddings = {}
else:
    raw_visual   = torch.load(os.path.join(args.data_dir, 'visual_embeddings.pt'), weights_only=False)
    orig_to_new  = dict(zip(item_id_map['item'], item_id_map['itemId']))
    visual_embeddings = {orig_to_new[k]: v for k, v in raw_visual.items() if k in orig_to_new}

# Specify the exact model
if args.use_seq_user:
    config = seqneumf_config
    config['use_seq_user'] = True
else:
    config = neumf_config

# DataLoader for training
maxlen = config.get('maxlen', 50)
sample_generator = SampleGenerator(ratings=ml1m_rating, visual_embeddings=visual_embeddings, maxlen=maxlen)
evaluate_data = sample_generator.evaluate_data

config['num_users']   = ml1m_rating['userId'].nunique()
config['num_items']   = ml1m_rating['itemId'].nunique()
config['num_epoch']   = args.num_epoch
config['batch_size']  = args.batch_size
config['use_cuda']    = args.use_cuda
config['visual_dim']  = 0 if args.no_visual else 768

if args.no_visual:
    config['alias'] += '_novisual'

config['model_dir']   = os.path.join(args.checkpoint_dir, '{}_Epoch{}_HR{:.4f}_NDCG{:.4f}.model')

if args.use_seq_user:
    engine = SeqNeuMFEngine(config)
else:
    engine = NeuMFEngine(config)

for epoch in range(config['num_epoch']):
    print('Epoch {} starts !'.format(epoch))
    print('-' * 80)
    train_loader = sample_generator.instance_a_train_loader(config['num_negative'], config['batch_size'])
    engine.train_an_epoch(train_loader, epoch_id=epoch)
    hit_ratio, ndcg = engine.evaluate(evaluate_data, epoch_id=epoch)
    engine.save(config['alias'], epoch, hit_ratio, ndcg)
