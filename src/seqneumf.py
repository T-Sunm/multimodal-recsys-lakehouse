import torch
from torch import nn


class SequentialEncoder(nn.Module):
    """Transformer-based encoder that produces a dynamic user representation
    from a sequence of interacted items, following the SASRec architecture.
    """

    def __init__(self, num_items, hidden_dim, maxlen, num_heads, num_layers, dropout):
        super().__init__()
        self.item_emb = nn.Embedding(num_items + 1, hidden_dim, padding_idx=0)
        self.pos_emb  = nn.Embedding(maxlen + 1,    hidden_dim, padding_idx=0)
        self.emb_dropout = nn.Dropout(p=dropout)

        encoder_layer = nn.TransformerEncoderLayer(
            d_model=hidden_dim,
            nhead=num_heads,
            dim_feedforward=hidden_dim,  # narrow FFN, same as original SASRec Conv1d(H→H)
            dropout=dropout,
            activation='relu',
            batch_first=True,
            norm_first=False,            # Post-LN, consistent with SASRec
        )
        self.transformer = nn.TransformerEncoder(
            encoder_layer,
            num_layers=num_layers,
            norm=nn.LayerNorm(hidden_dim, eps=1e-8),
        )

    def forward(self, item_seq):
        """
        Args:
            item_seq: LongTensor of shape (B, L) — item IDs, 0 = padding
        Returns:
            Tensor of shape (B, hidden_dim) — user representation at the last step
        """
        device = item_seq.device

        # Item + positional embeddings (scaled, padding positions stay zero)
        positions = torch.arange(1, item_seq.size(1) + 1, device=device) \
                        .unsqueeze(0).expand_as(item_seq)
        positions = positions * (item_seq != 0)   # zero out padding positions

        x = self.item_emb(item_seq) * (self.item_emb.embedding_dim ** 0.5)
        x = x + self.pos_emb(positions)
        x = self.emb_dropout(x)

        # Causal mask: prevent attending to future positions
        seq_len = x.size(1)
        causal_mask = nn.Transformer.generate_square_subsequent_mask(seq_len, device=device)

        # Padding mask: prevent attending to padded positions
        padding_mask = (item_seq == 0)

        x = self.transformer(x, mask=causal_mask, is_causal=True, src_key_padding_mask=padding_mask)

        return x[:, -1, :]   # (B, hidden_dim) — representation at the last non-padding step


class SeqNeuMF(nn.Module):
    """Neural Matrix Factorization with optional Sequential User Encoder.

    When ``use_seq_user=True``: user representation is derived from the item
    interaction history via a Transformer (SeqNeuMF variant).
    When ``use_seq_user=False``: falls back to static user ID embeddings
    (standard NeuMF behaviour, useful for ablation studies).
    """

    def __init__(self, config):
        super().__init__()
        self.num_users    = config['num_users']
        self.num_items    = config['num_items']
        self.latent_dim_mf  = config['latent_dim_mf']
        self.latent_dim_mlp = config['latent_dim_mlp']
        self.use_seq_user   = config.get('use_seq_user', True)
        hidden_seq          = config.get('seq_hidden_units', 50)

        # --- User branch ---
        if self.use_seq_user:
            self.seq_encoder  = SequentialEncoder(
                num_items  = self.num_items,
                hidden_dim = hidden_seq,
                maxlen     = config.get('maxlen', 50),
                num_heads  = config.get('num_heads', 1),
                num_layers = config.get('num_blocks', 2),
                dropout    = config.get('dropout_rate', 0.2),
            )
            # Project Transformer output → NeuMF latent spaces
            self.proj_user_mf  = nn.Linear(hidden_seq, self.latent_dim_mf)
            self.proj_user_mlp = nn.Linear(hidden_seq, self.latent_dim_mlp)
        else:
            self.user_emb_mf  = nn.Embedding(self.num_users, self.latent_dim_mf)
            self.user_emb_mlp = nn.Embedding(self.num_users, self.latent_dim_mlp)

        # --- Item branch ---
        self.item_emb_mf  = nn.Embedding(self.num_items, self.latent_dim_mf)
        self.item_emb_mlp = nn.Embedding(self.num_items, self.latent_dim_mlp)

        # Visual fusion: [item_id_emb | visual_feat] → latent_dim
        visual_dim = config.get('visual_dim', 768)
        self.use_visual = visual_dim > 0
        if self.use_visual:
            self.visual_fusion_mf  = nn.Linear(self.latent_dim_mf  + visual_dim, self.latent_dim_mf)
            self.visual_fusion_mlp = nn.Linear(self.latent_dim_mlp + visual_dim, self.latent_dim_mlp)

        # --- MLP tower ---
        self.fc_layers = nn.ModuleList([
            nn.Linear(in_size, out_size)
            for in_size, out_size in zip(config['layers'][:-1], config['layers'][1:])
        ])

        # --- Output head ---
        self.output_layer = nn.Linear(config['layers'][-1] + self.latent_dim_mf, 1)
        self.sigmoid = nn.Sigmoid()

        # Weight initialisation
        if config.get('weight_init_gaussian', False):
            for module in self.modules():
                if isinstance(module, (nn.Embedding, nn.Linear)):
                    nn.init.normal_(module.weight.data, mean=0.0, std=0.01)

    def forward(self, user_ids, item_seqs, item_ids, visual_feats):
        """
        Args:
            user_ids   : LongTensor (B,)       — user indices (used when use_seq_user=False)
            item_seqs  : LongTensor (B, L)     — history sequences (used when use_seq_user=True)
            item_ids   : LongTensor (B,)       — candidate item indices
            visual_feats: Tensor   (B, V)      — visual embeddings (empty when use_visual=False)
        Returns:
            Tensor (B, 1) — predicted interaction probability
        """
        # User representation
        if self.use_seq_user:
            user_seq_repr  = self.seq_encoder(item_seqs)       # (B, hidden_seq)
            user_vec_mf    = self.proj_user_mf(user_seq_repr)  # (B, latent_dim_mf)
            user_vec_mlp   = self.proj_user_mlp(user_seq_repr) # (B, latent_dim_mlp)
        else:
            user_vec_mf  = self.user_emb_mf(user_ids)
            user_vec_mlp = self.user_emb_mlp(user_ids)

        # Item representation (with optional visual fusion)
        item_vec_mf  = self.item_emb_mf(item_ids)
        item_vec_mlp = self.item_emb_mlp(item_ids)
        if self.use_visual:
            item_vec_mf  = self.visual_fusion_mf( torch.cat([item_vec_mf,  visual_feats], dim=-1))
            item_vec_mlp = self.visual_fusion_mlp(torch.cat([item_vec_mlp, visual_feats], dim=-1))

        # GMF branch (element-wise product)
        mf_out = torch.mul(user_vec_mf, item_vec_mf)

        # MLP branch (concatenate → dense layers)
        mlp_out = torch.cat([user_vec_mlp, item_vec_mlp], dim=-1)
        for layer in self.fc_layers:
            mlp_out = torch.relu(layer(mlp_out))

        # Combine and predict
        combined = torch.cat([mf_out, mlp_out], dim=-1)
        return self.sigmoid(self.output_layer(combined))