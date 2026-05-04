Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices


models/
├── bronze/
│   ├── sources.yml                   # 4 sources: interactions, items, likes_views, visual_embeddings
│   ├── bronze_interactions.sql       # pairs.csv → rename + ingested_at
│   ├── bronze_items.sql              # titles.csv LEFT JOIN likes_views (merged)
│   └── bronze_visual_embeddings.sql  # visual_embeddings.parquet → passthrough
├── silver/
│   ├── schema.yml
│   ├── silver_interactions.sql       # cast INT, timestamp ms÷1000, dedup
│   ├── silver_items.sql              # clean title, like_log/view_log
│   ├── silver_visual_embeddings.sql  # validate 768-dim
│   └── silver_user_sequences.sql     # sliding window collect_list
└── gold/
    ├── schema.yml
    ├── gold_item_features.sql        # ← MỚI: JOIN items + embeddings (thay gold_item_embeddings)
    ├── gold_popularity_stats.sql     # weighted popularity score
    └── gold_training_samples.sql     # ML samples, NO cover_path filter
