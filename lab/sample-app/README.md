# orders-api

Takes orders from partner stores and writes them to our database.

- `python3 app.py --port 9000` runs the service.
- `GET /stats` shows counters.
- `python3 partners.py --target http://127.0.0.1:9000` replays a sale-day burst from our partners.

During sales, partners get a lot of `503`s, and the small shops complain that their orders fail even though they barely send anything.
