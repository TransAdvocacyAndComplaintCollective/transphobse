from utils.news_media import fetch_news_media_data


media_data = fetch_news_media_data()
print(media_data)
for media, data in media_data.items():
    print(media)
    print(data)