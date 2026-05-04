import os
import shutil
import pandas as pd

raw_video_dir = 'data/microlens-5k/MicroLens-100k_videos_Part_1'
cover_dir = 'data/microlens-5k/covers'
target_video_dir = 'data/microlens-5k/videos'

# Tạo thư mục đích nếu chưa có
os.makedirs(target_video_dir, exist_ok=True)

# BƯỚC 1: Kiểm tra cover và tìm các video tương ứng (Lọc ra ~1k3 video)
print("\n--- BƯỚC 1: KẾT NỐI COVER VÀ VIDEO ---")
cover_files = [f for f in os.listdir(cover_dir) if f.endswith(('.jpg', '.png'))]
print(f"Đang kiểm tra {len(cover_files)} file ảnh cover trong '{cover_dir}'...")

initial_found_videos = []
missing_videos = []

for cover in cover_files:
    basename = cover.rsplit('.', 1)[0]
    video_path = os.path.join(raw_video_dir, f"{basename}.mp4")
    
    if os.path.exists(video_path):
        initial_found_videos.append(basename)

print(f"=> Lọc lần 1: Tìm thấy {len(initial_found_videos)} video có cover hợp lệ.")

if not initial_found_videos:
    print("\n(Cảnh báo: Không tìm thấy video nào. Có thể dữ liệu đã được di chuyển ở lần chạy trước. Vui lòng giải nén lại data gốc để chạy flow này.)")

# BƯỚC 2: Từ danh sách ~1k3 video, tìm Top 5 users
print("\n--- BƯỚC 2: TÌM TOP 5 USERS TƯƠNG TÁC ---")
pairs_df = pd.read_csv('data/microlens-5k/pairs.csv')

# Lọc dataframe chỉ lấy các tương tác thuộc danh sách initial_found_videos
found_videos_set = set(initial_found_videos)
pairs_df['item_str'] = pairs_df['item'].astype(str)
filtered_pairs = pairs_df[pairs_df['item_str'].isin(found_videos_set)]

# Lấy 5 user có nhiều tương tác nhất
if not filtered_pairs.empty:
    top_users = filtered_pairs['user'].value_counts().nlargest(5).index.tolist()
else:
    top_users = []
print(f"=> Tự động tìm thấy Top 5 users: {top_users}")

# Danh sách các video mà 5 users này đã xem
top_user_items = set(filtered_pairs[filtered_pairs['user'].isin(top_users)]['item_str'].unique())

# BƯỚC 3: Lọc lần cuối và Di chuyển dữ liệu
print("\n--- BƯỚC 3: LỌC VIDEO THEO TOP USERS VÀ DI CHUYỂN ---")
final_found_videos = []
for v in initial_found_videos:
    if v in top_user_items:
        final_found_videos.append(v)

print(f"=> GIỮ LẠI (chỉ chứa video của top 5 user): {len(final_found_videos)} video")

print(f"\nĐang di chuyển {len(final_found_videos)} video hợp lệ sang '{target_video_dir}'...")
for basename in final_found_videos:
    src_video = os.path.join(raw_video_dir, f"{basename}.mp4")
    dst_video = os.path.join(target_video_dir, f"{basename}.mp4")
    if os.path.exists(src_video) and not os.path.exists(dst_video):
        shutil.move(src_video, dst_video)

print("\nHoàn tất toàn bộ pipeline!")
