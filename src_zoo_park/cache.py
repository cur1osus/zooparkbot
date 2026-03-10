from cachetools import TTLCache

text_cache = TTLCache(maxsize=500, ttl=5000)
button_cache = TTLCache(maxsize=500, ttl=5000)
value_cache = TTLCache(maxsize=500, ttl=5000)
photo_cache = TTLCache(maxsize=100, ttl=5000)
plot_cache = TTLCache(maxsize=20, ttl=30)
