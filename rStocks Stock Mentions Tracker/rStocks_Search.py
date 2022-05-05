from psaw import PushshiftAPI
import datetime
import config
import psycopg2
import psycopg2.extras


# Connect to rstocks_db database.
connection = psycopg2.connect(
    host=config.DB_HOST,
    database=config.DB_NAME,
    user=config.DB_USER,
    password=config.DB_PASS,
)

cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

api = PushshiftAPI()

# Get posts after date "datetime" on r/Stocks subreddit.
start_time = int(datetime.datetime(2022, 4, 30).timestamp())
submissions = api.search_submissions(
    after=start_time,
    subreddit="stocks",
    filter=["url", "author", "title", "subreddit"],
)

# Get posts with cashtags in the title and put into SQL database.
for submission in submissions:
    words = submission.title.split()
    cashtags = list(
        set(
            filter(
                lambda word: word.lower().startswith("$") and word[1:2].isalpha(), words
            )
        )
    )

    if len(cashtags) > 0:
        for cashtag in cashtags:
            submit_time = datetime.datetime.fromtimestamp(
                submission.created_utc
            ).isoformat()

            try:
                cursor.execute(
                    "INSERT INTO mention (dt, symbol, message, url) VALUES (%s, %s, %s, %s)",
                    (submit_time, cashtag[1:], submission.title, submission.url),
                )
                connection.commit()

            except Exception as e:
                print(e)
                connection.rollback()
