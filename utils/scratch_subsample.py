import argparse
import os
import shutil

import pandas as pd


def parse_args():
    parser = argparse.ArgumentParser(description="Subsample microlens-50k dataset to 5k items")
    parser.add_argument("--src_dir", type=str, default=r"e:\AIO\Project\MM-ShortVideo-Rec\data\microlens-50k",
                        help="Path to source dataset directory (microlens-50k)")
    parser.add_argument("--dst_dir", type=str, default=r"e:\AIO\Project\MM-ShortVideo-Rec\data\microlens-5k",
                        help="Path to output dataset directory (microlens-5k)")
    return parser.parse_args()


args = parse_args()

data_dir = args.src_dir
new_data_dir = args.dst_dir
pairs_path = os.path.join(data_dir, "pairs.csv")
titles_path = os.path.join(data_dir, "titles.csv")
likes_path = os.path.join(data_dir, "likes_and_views.txt")

# Đọc toàn bộ tương tác
df = pd.read_csv(pairs_path)
print(f"Tổng số tương tác (gốc): {len(df)}")
print(f"Tổng số User (gốc): {df['user'].nunique()}")
print(f"Tổng số Item/Video (gốc): {df['item'].nunique()}")

# 1. Chọn 5,000 Item phổ biến nhất CÓ ảnh bìa
covers_dir = os.path.join(data_dir, "covers")
item_counts = df['item'].value_counts()
valid_5k_items = []

print("Đang tìm 5,000 items có chứa ảnh bìa...")
for item_id in item_counts.index:
    # Kiểm tra xem file ảnh có tồn tại thực sự không
    if os.path.exists(os.path.join(covers_dir, f"{item_id}.jpg")):
        valid_5k_items.append(item_id)
        if len(valid_5k_items) == 5000:
            break

if len(valid_5k_items) < 5000:
    print(f"Cảnh báo: Chỉ tìm thấy {len(valid_5k_items)} items có ảnh!")
else:
    print("Đã gom đủ 5000 items có ảnh!")

df_5k = df[df['item'].isin(valid_5k_items)]

# Nếu sau khi giảm item mà có những user chỉ còn <= 1 tương tác (không đủ chia train/test loo), 
# lọc tiếp giữ lại những user có tối thiểu 2 tương tác.
user_counts = df_5k['user'].value_counts()
valid_users = user_counts[user_counts >= 2].index

# Cắt bớt còn tối đa 20,000 users (ưu tiên lấy những user có nhiều tương tác nhất)
if len(valid_users) > 20000:
    valid_users = valid_users[:20000]

df_5k = df_5k[df_5k['user'].isin(valid_users)]

print("\n--- SAU KHI LỌC ---")
print(f"Tổng số tương tác (mới): {len(df_5k)}")
print(f"Tổng số User (mới): {df_5k['user'].nunique()}")
print(f"Tổng số Item/Video (mới): {df_5k['item'].nunique()}")

# Cập nhật danh sách item thực sự có trong tập 5k sau khi lọc user
final_items = set(df_5k['item'].unique())

# Đọc và lọc các metadata
df_titles = pd.read_csv(titles_path)
df_likes = pd.read_csv(likes_path, sep='\t', header=None, names=['item', 'likes', 'views'])

df_titles_5k = df_titles[df_titles['item'].isin(final_items)]
df_likes_5k = df_likes[df_likes['item'].isin(final_items)]

# Lưu ra thư mục dataset mới
os.makedirs(new_data_dir, exist_ok=True)
df_5k.to_csv(os.path.join(new_data_dir, "pairs.csv"), index=False)
df_titles_5k.to_csv(os.path.join(new_data_dir, "titles.csv"), index=False)
df_likes_5k.to_csv(os.path.join(new_data_dir, "likes_and_views.txt"), sep='\t', header=False, index=False)

# Copy ảnh bìa sang folder mới
new_covers_dir = os.path.join(new_data_dir, "covers")
os.makedirs(new_covers_dir, exist_ok=True)
print("Đang copy ảnh bìa sang folder mới...")
for item_id in final_items:
    src_path = os.path.join(covers_dir, f"{item_id}.jpg")
    dst_path = os.path.join(new_covers_dir, f"{item_id}.jpg")
    if os.path.exists(src_path):
        shutil.copy2(src_path, dst_path)

print(f"\nĐã xuất toàn bộ dataset 5k (bao gồm metadata và ảnh) thành công tại: {new_data_dir}")
