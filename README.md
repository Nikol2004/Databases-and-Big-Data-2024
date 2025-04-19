# Databases & Big Data: Spotify Analysis

This project explores one of the world’s most popular streaming platforms—**Spotify**—using a structured database built from scratch. With over 10,000 records on the most-streamed songs, we dove deep into the data to clean, model, and query meaningful musical trends.

---

### What We Built

We designed a relational database using our custom ER diagram featuring:

- `Track`: Core song info (name, cover, artist count)
- `Artist`: All unique artist names
- `TrackProfile`: Musical features like BPM, energy, speechiness, etc.
- `StreamingInfo`: Cross-platform performance metrics
- `Released_By`: A junction table linking artists and tracks

> Built in PostgreSQL, and populated with Python data cleaning & preprocessing code.

---

### What We Fixed

We cleaned messy data and handled:

- Missing values (replaced or randomly filled when appropriate)
- Strange Unicode characters
- Duplicate rows (via string normalization + sorting by streams)
- Inconsistent numerical values (e.g., "in_deezer_playlists" with commas)

The result? A clean and query-ready dataset.

---

### Our Insights

We wrote several exploratory SQL queries to uncover interesting trends:

- **Query 1**: Tracks how average BPM and danceability changed over the years  
- **Query 4**: Analyzes how tempo (BPM) affects popularity based on streams  
- **Query 5**: Examines whether high-liveliness tracks also have album cover art, linking visual and sonic appeal

These queries tell stories about the intersection of music, data, and culture.

---

### Team Q

Nikol Tushaj  
Rajla Culli  
Chloe Monique Quevedo  
Lina Kolevska

---

Thanks for reading — and happy streaming! 🎶
