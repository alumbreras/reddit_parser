Parses a json file where every entry represents a reddit comment.
Store it in a mysql database with three tables: threads, users, posts.

 - parser_reddit.py: main script
 - dbmanager.py: functions to interact with the MySQL database
 - input/:  copy the dataset here.
 - output/: database containing the parsed dataset.
   
### Instructions

Download a reddit json file where every json entry represents a comment, such as:
https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/

Copy the file in `/output/` and modify this line in `parser_reddit.py` accordingly:

    input_file = join(filepath, "input", "RC_2015-01", "reddit")

Every line should be contain at least the following fields:

    {"subreddit":"AskMen",
    "subreddit_id":"t5_2s30g",
    "id":"cnasd6x",
    "name":"t1_cnasd6x",
    "parent_id":"t1_cnapn0k",
    "link_id":"t3_2qyhmp",
    "author":"TheDukeofEtown",
    "body":"I can't agree with passing the blame, ... ",
    "created_utc":"1420070668"}

Then run:

    python parser_reddit.py
   
and go to sleep. Your database will be in output/ in the morning. (about 8h for a 36GB file)

### Notes
The parser was made to parse the dataset linked above. In that dataset, the post that opened the thread (called submission) is not included. 

Imagine, for instance, that the parent of comment `t1_cnasd8fx` is the link `t3_hfgt8f` (t3... are the submissions), but there is no entry associared to `t3_hfgt8f`. The script creates an empty post entry in the database with id `t3_hfgt8f`. 

This is necessary if we want to use the database to study the structure of the conversation, since `t3_...` are the roots of the conversation trees.