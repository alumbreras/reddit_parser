Parses a json file where every entry represents a reddit comment.
Store it in a mysql database with three tables: threads, users, posts.

 - parser_reddit.py: main script
 - dbmanager.py: functions to interact with the MySQL database
 - input/:  copy the dataset here.
 - output/: database containing the parsed dataset.
   
### Instructions

Download a reddit json file where every json entry represents a comment, such as:
https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/

    {"gilded":0,
    "author_flair_text":"Male",
    "author_flair_css_class":"male",
    "retrieved_on":1425124228,
    "ups":3,
    "subreddit_id":"t5_2s30g",
    "edited":false,
    "controversiality":0,
    "parent_id":"t1_cnapn0k",
    "subreddit":"AskMen",
    "body":"I can't agree with passing the blame, ... ",
    "created_utc":"1420070668",
    "downs":0,"score":3,
    "author":"TheDukeofEtown",
    "archived":false,
    "distinguished":null,
    "id":"cnasd6x",
    "score_hidden":false,
    "name":"t1_cnasd6x",
    "link_id":"t3_2qyhmp"}

Then run:

    python parser_reddit.py
   
and go to sleep. Your database will be in output/ in the morning. (about 8h for a 36GB file)