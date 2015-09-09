# -*- coding: utf-8 -*-
"""
Parses a json file where every entry represents a reddit comment.
Store it in a mysql database with three tables: threads, users, posts.
@author: Alberto Lumbreras
"""

import os
import dbmanager as dbmanager
from os.path import join
import json

def parse_reddit():
    '''
    Parse reddit from json files and store them in a SQLite database
    '''
    filepath = os.path.dirname(os.path.abspath(__file__))
    db = dbmanager.DBmanager(join('output', 'reddit.db'))
    input_file = join(filepath, "input", "RC_2015-01", "reddit")
    

    nposts = sum(1 for line in open(input_file, 'r'))
    with open(input_file, 'r') as f:
        
        # Every line is a post        
        nforums=0
        for i, line in enumerate(f):
             

            if i > 0.25*nposts: 
                break
            
            user= "anonymous"
            date = None
            parent = None
            linkid = None
            threadid = None
            forum = None
            
            d = json.loads(line)
            postid = d["name"] # t1_cnas8zv
            text = d["body"]
            user = d["author"]
            threadid = d["link_id"] # t3_... (id of root link)
            date = d["created_utc"] # seconds   
            parent = d["parent_id"] # t3_ if first level comment; else t1_... 
            forumid = d["subreddit_id"] #t5_...
            forum = d["subreddit"] # showerthoughts
            id_ = d["id"] # cnas8zv
    
            root = threadid
    
            try:
                
                if not db.exists_thread(threadid):
    
                    # Insert the root posts, even if we do not have its full info
                    db.insert_post(postid=root, 
                                   user="root", 
                                   thread=threadid,
                                   subject=None, 
                                   parent=None, 
                                   date=None, 
                                   text=None)    
                           
                    db.insert_thread(threadid, root, None, forum)
                    nforums += 1
            except Exception as e:
                pass
    
            if not db.exists_user(user):
                username = user 
                db.insert_user(userid=user, username=username)
    
            db.insert_post(postid=postid, 
                           user=user, 
                           thread=threadid,
                           subject=None, 
                           parent=parent, 
                           date=date, 
                           text=text)    
                                                    
            #1 000 000
            if i%100000 == 0:
                print "Processed", i, "/", nposts , 100.0*i/nposts, "posts/forum", 1.0*i/nforums
            if i%20000000 == 0:
                print "Processed", i, "/", nposts , 100.0*i/nposts, "posts/forum", 1.0*i/nforums
                db.dbcommit()
                print "....................................."
        
        db.dbcommit()
        
        # Delete threads where some post has a parent which is not in the database
        # unconnected trees
        threads = db.get_all_threads().fetchall()
        nthreads = len(threads)
        print "N threads:", len(threads)
        print "N posts:", len(db.get_all_posts().fetchall())    
        
        complete_threads = 0
        ok_posts = 0
        
        for thread in threads:
            threadid = thread['threadid']
            posts = db.get_posts_in_thread(threadid).fetchall()
            
            for post in posts:
                if post['postid'] == threadid: continue # skip root
                if not db.exists_post(post['parent']):
                    db.delete_thread(threadid)
                    complete_threads -= 1
                    break
                else:
                    ok_posts +=1
                    
            complete_threads +=1
        
        db.dbcommit()  

        print "\nAfter cleaning:"
        print "Complete threads", complete_threads, "/", nthreads
        print "Post with parent in db", ok_posts
        print "N threads final:", len(db.get_all_threads().fetchall())
        print "N posts final:", len(db.get_all_posts().fetchall())            
                  
    
        # Set (fake) date of root
        # to one second before the first post
        for thread in threads:
            root = thread['root']
            posts = db.get_posts_in_thread(root).fetchall()
            date = int(posts[1]['date'])-1 # root is posts[0] since NULL is a 0
            db.update_post_date(root, date)    
            
        db.dbcommit()
        
        
parse_reddit()
