'''
Set up the tables of the database
'''
import sqlite3 as sqlite
import os

class DBmanager():
    def __init__(self, dbname):
        thisfilepath = os.path.dirname(os.path.realpath(__file__))
        dbrelpath = ''
        self.filepath = os.path.join(thisfilepath, dbrelpath, dbname)

        # Create tables if do not exist
        if not os.path.exists(self.filepath):
            self.con=sqlite.connect(self.filepath)
            self.con.row_factory = sqlite.Row
            self.cursor = self.con.cursor()
            self.create_tables()
            self.dbcommit()
            
        # Else, connect to the db
        else:
            self.con=sqlite.connect(self.filepath)
            self.con.row_factory = sqlite.Row
            self.cursor = self.con.cursor()
            
            
    def __del__(self):
        self.con.close()
        
    def dbcommit(self):
        self.con.commit()
        
    def dbdelete(self):
        if os.path.exists(self.filepath):
            os.remove(self.filepath)

    def create_tables(self):
        '''Init empty tables to store threads, users and posts info''' 
        
        self.cursor.execute("""create table threads(
                            threadid TEXT   PRIMARY KEY, 
                            title    TEXT, 
                            forum     TEXT,
                            root     TEXT
                            )""")
        
        self.cursor.execute("""create table users(
                            userid TEXT PRIMARY KEY, 
                            username TEXT
                            )""")
        
        self.cursor.execute("""create table posts(
                            postid TEXT     PRIMARY KEY, 
                            user   TEXT     REFERENCES users(userid), 
                            thread TEXT     REFERENCES threads(threadid),
                            subject TEXT, 
                            parent TEXT     REFERENCES users(userid), 
                            date   TEXT, 
                            text   TEXT
                            )""")
        
        self.cursor.execute('create index posts_user_idx on posts(user)')
        self.cursor.execute('create index posts_thread_idx on posts(thread)')
        self.cursor.execute('create index posts_parent_idx on posts(parent)')
        
        
    def insert_post(self, postid=None, user=None, thread=None, subject=None,
                          parent=None, date=None, text=None):
        self.cursor.execute("""INSERT INTO 
                            posts(
                            postid, 
                            user, 
                            thread,
                            subject,
                            parent, 
                            date, 
                            text) 
                            VALUES 
                            (?,?,?,?,?,?,?)"""
                            , (postid, user, thread, subject, 
                               parent, date, text))

    def insert_thread(self, threadid, root, title, forum, ignore=False):
        if not ignore:
            self.cursor.execute("""INSERT INTO
                                threads(threadid, title, forum, root)
                                VALUES 
                                (?,?,?,?)"""
                                , (threadid, title, forum, root))
        else:
            self.cursor.execute("""INSERT IGNORE INTO
                                threads(threadid, title, forum, root)
                                VALUES 
                                (?,?,?,?)"""
                                , (threadid, title, forum, root))            

    def insert_user(self, userid, username=None):
        self.cursor.execute('''INSERT INTO
                            users(userid, username)
                            VALUES
                            (?,?)'''
                            ,(userid, username))

    def set_threads(self):
        '''Propagate thread information down the tree
           Consider that the threadid is the postid of the root post
           Use it when no other threadid provided
           '''
        
        def propagate_thread(threadid, node):
            '''Recursive propagation of threadid'''
            
            # Update info in its children
            self.cursor.execute("""UPDATE posts SET thread=? 
                                WHERE parent=?""", (threadid, node))

            # Let children do the same with their children
            cursor = self.cursor.execute("""SELECT postid FROM posts 
                                         WHERE parent=?""", (node,))
            
            for row in cursor.fetchall():
                print node, row['postid']
                propagate_thread(threadid, row['postid'])
                
          
        # Find roots      
        cursor = self.cursor.execute("""SELECT postid, subject FROM posts 
                                     WHERE parent IS NULL""")
        # Tell all descendants that postid=root
        for row in cursor.fetchall():
            print "propagating from root...", row['postid']

            self.cursor.execute("""UPDATE posts SET thread=? WHERE postid=?""", 
                                   (row['postid'],row['postid']))
            self.insert_thread(row['postid'], row['postid'], row['subject'])
            
            propagate_thread(row['postid'], row['postid'])
            
  

    def exists_thread(self, threadid):
        '''
        Returns True if table 'threads' contains row with threadid;
        False otherwise
        '''
        u = self.cursor.execute("SELECT threadid FROM threads WHERE threadid=?" 
                             , (threadid,)).fetchone()
        if u is None:
            return False
        else:
            return True
        
    def exists_user(self, userid):
        '''
        Returns True if table 'users' contains row with userid;
        False otherwise
        '''
        u = self.cursor.execute("SELECT userid FROM users WHERE userid=?" 
                             , (userid,)).fetchone()
        if u is None:
            return False
        else:
            return True
        
    def exists_post(self, postid):
        '''
        Returns True if table 'posts' contains row with userid;
        False otherwise
        '''
        u = self.cursor.execute("SELECT postid FROM posts WHERE postid=?" 
                             , (postid,)).fetchone()
        if u is None:
            return False
        else:
            return True
        
    def get_all_threads(self):
        return self.cursor.execute("SELECT * FROM threads")
    
    def get_all_users(self):
        return self.cursor.execute("SELECT * FROM users")

    def get_all_posts(self):
        return self.cursor.execute("SELECT * FROM posts")
        
    def get_threads_in_forum(self, forumid):
        return self.cursor.execute('''SELECT * FROM threads 
                                   WHERE forum=?'''
                                   ,(forumid,))
                                   
    def get_users_in_thread(self, threadid):
        return self.cursor.execute('''SELECT DISTINCT user FROM posts 
                                   WHERE thread=?'''
                                   ,(threadid,))

    def get_posts_in_thread(self, threadid):
        """
        Return every post_id in thread
        
        Parameters
        ----------
        threadid : string
          Thread id.
        """
        return self.cursor.execute("""SELECT * FROM posts 
                           WHERE thread=? ORDER BY DATE ASC"""
                           ,(threadid,))
        
    def get_user_threads(self, userid):
        return self.cursor.execute("""SELECT DISTINCT thread FROM posts WHERE user=?"""
                                   , (userid,))
        
    def get_username(self, userid):
        return self.cursor.execute("""SELECT username FROM users 
                                       WHERE userid=?"""
                                    ,(userid,))
         
    def get_thread_title(self, threadid):
        return self.cursor.execute("""SELECT title FROM threads 
                                    WHERE threadid=?"""
                                    ,(threadid,))
    
    def delete_thread(self, threadid):
        self.cursor.execute("""DELETE FROM threads WHERE threadid=?"""
                            , (threadid,))

    def update_post_date(self, postid, date):
        self.cursor.execute("""UPDATE posts SET date=? WHERE postid=?"""
                            , (date, postid))
                            
if __name__=="__main__":
    dbfile = "test.db"
    db = DBmanager(dbfile)
    db.insert_post(1, 1, 3, 4, None, 4, "test")
    db.dbcommit()