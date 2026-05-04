"""Mock MicroLens items conforming to the data-contract schema.

All items are normalized into the canonical shape defined in
`.specify/memory/data-contract.md`.  Missing optional fields are
deliberately included in some records to exercise graceful fallback
rendering in the UI.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Canonical shape reminder (do not deviate):
#   id, title, cover, preview_frames, video, likes, views, tags, meta
# ---------------------------------------------------------------------------

MOCK_ITEMS: list[dict] = [
    {
        "id": "item_001",
        "title": "Golden Hour Timelapse – City Skyline at Dusk",
        "cover": "https://picsum.photos/seed/ml001/320/180",
        "preview_frames": [
            "https://picsum.photos/seed/ml001a/320/180",
            "https://picsum.photos/seed/ml001b/320/180",
        ],
        "video": None,
        "likes": 14_200,
        "views": 98_500,
        "tags": ["timelapse", "cityscape", "photography"],
        "meta": {"duration_s": 42, "fps": 24},
    },
    {
        "id": "item_002",
        "title": "Street Food Tour – Tokyo Night Markets",
        "cover": "https://picsum.photos/seed/ml002/320/180",
        "preview_frames": ["https://picsum.photos/seed/ml002a/320/180"],
        "video": None,
        "likes": 32_700,
        "views": 210_000,
        "tags": ["food", "travel", "tokyo"],
        "meta": {"duration_s": 87},
    },
    {
        "id": "item_003",
        "title": "Acoustic Guitar Cover – Rainy Day Playlist",
        "cover": "https://picsum.photos/seed/ml003/320/180",
        "preview_frames": [],
        "video": None,
        "likes": 5_400,
        "views": 27_300,
        "tags": ["music", "guitar", "acoustic"],
        "meta": None,
    },
    {
        "id": "item_004",
        "title": "Morning Yoga Flow – 10-Minute Energiser",
        "cover": None,                          # ← cover missing on purpose
        "preview_frames": ["https://picsum.photos/seed/ml004a/320/180"],
        "video": None,
        "likes": 8_900,
        "views": 44_100,
        "tags": ["yoga", "fitness", "wellness"],
        "meta": {"duration_s": 600},
    },
    {
        "id": "item_005",
        "title": "DIY Terrarium Build – Step by Step",
        "cover": "https://picsum.photos/seed/ml005/320/180",
        "preview_frames": [
            "https://picsum.photos/seed/ml005a/320/180",
            "https://picsum.photos/seed/ml005b/320/180",
            "https://picsum.photos/seed/ml005c/320/180",
        ],
        "video": None,
        "likes": None,                           # ← likes missing on purpose
        "views": 19_800,
        "tags": ["diy", "plants", "crafts"],
        "meta": {"duration_s": 312},
    },
    {
        "id": "item_006",
        "title": "Drone Footage – Norwegian Fjords",
        "cover": "https://picsum.photos/seed/ml006/320/180",
        "preview_frames": ["https://picsum.photos/seed/ml006a/320/180"],
        "video": None,
        "likes": 61_500,
        "views": 340_000,
        "tags": ["drone", "nature", "norway", "travel"],
        "meta": {"duration_s": 130, "fps": 30},
    },
    {
        "id": "item_007",
        "title": "Quick Pasta Recipe – Ready in 15 Minutes",
        "cover": "https://picsum.photos/seed/ml007/320/180",
        "preview_frames": [],
        "video": None,
        "likes": 22_100,
        "views": None,                           # ← views missing on purpose
        "tags": ["cooking", "recipe", "pasta"],
        "meta": {"duration_s": 95},
    },
    {
        "id": "item_008",
        "title": "Cat Compilation – Funniest Moments of 2024",
        "cover": "https://picsum.photos/seed/ml008/320/180",
        "preview_frames": [
            "https://picsum.photos/seed/ml008a/320/180",
            "https://picsum.photos/seed/ml008b/320/180",
        ],
        "video": None,
        "likes": 88_300,
        "views": 520_000,
        "tags": ["cats", "funny", "animals"],
        "meta": {"duration_s": 205},
    },
    {
        "id": "item_009",
        "title": "Ambient Lo-Fi Study Session – 1 Hour",
        "cover": None,                           # ← cover missing on purpose
        "preview_frames": [],
        "video": None,
        "likes": None,                           # ← both missing on purpose
        "views": None,
        "tags": ["lofi", "music", "study"],
        "meta": None,
    },
    {
        "id": "item_010",
        "title": "Minimalist Desk Setup Tour – 2025 Edition",
        "cover": "https://picsum.photos/seed/ml010/320/180",
        "preview_frames": ["https://picsum.photos/seed/ml010a/320/180"],
        "video": None,
        "likes": 17_600,
        "views": 93_400,
        "tags": ["tech", "setup", "productivity"],
        "meta": {"duration_s": 480},
    },
]


def get_mock_items() -> list[dict]:
    """Return a copy of all mock items (safe for mutation by callers)."""
    return list(MOCK_ITEMS)
