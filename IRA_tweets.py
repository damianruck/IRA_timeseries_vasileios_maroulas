import pandas as pd
import os

class IRA_tweets:
    def __init__(self):
        
        self.intersect = ['hashtags', 'is_retweet', 'quote_count', 'reply_count', 'retweet_count']
        
        # load column names from ira data and matched column names from rtweet data
        self.ira_cols = ['account_creation_date', 'account_language', 'follower_count',
           'in_reply_to_tweetid', 'in_reply_to_userid',
           'quoted_tweet_tweetid', 'retweet_tweetid', 'retweet_userid',
           'tweet_language', 'tweet_text', 'tweet_time',
           'tweetid', 'urls', 'user_display_name', 'user_mentions',
           'user_profile_description', 'user_profile_url',
           'user_reported_location', 'user_screen_name', 'userid']

        self.rtweet_cols=['account_created_at', 'account_lang','followers_count','reply_to_status_id', 'reply_to_user_id',
            'quoted_status_id', 'retweet_status_id', 'retweet_user_id','lang','text','created_at','status_id',
            'urls_expanded_url','name','mentions_user_id','description','profile_expanded_url','location',
            'screen_name','user_id']
        
        # full
        self.rtweet_cols_fullset = pd.read_csv('samplehl.csv',nrows=1).columns
        
        self.data = pd.read_csv('ira_tweets_csv_hashed.csv',
                usecols=self.intersect+self.ira_cols)#, nrows=100000)

        #if 'tweet_time' in columns_of_interest: #convert to datetime format, include only date (not time)
        self.data['tweet_time'] = pd.to_datetime(self.data['tweet_time'])
        
    def english_only_tweets(self):
        def filter_out_nonenglish_tweets(df):
            df=df[(df['account_language'] == 'en') & (df['tweet_language'] == 'en')]
            return df

        self.data = (self.data.pipe(filter_out_nonenglish_tweets).
            drop(['account_language','tweet_language'],1))
        
    def round_dates_to_weekstarting(self):
        def round_dates_to_week_starting_function(df):
            df['tweet_time'] = df['tweet_time'] - pd.Timedelta('1d') * df['tweet_time'].dt.dayofweek
            df['tweet_time'] = pd.to_datetime(df['tweet_time'].dt.date)
            return df

        self.data = self.data.pipe(round_dates_to_week_starting_function)
        
        
    def get_number_of_weeks_account_posted(self):
        self.number_of_weeks_account_posted = (self.data.groupby(['user_screen_name'],as_index=False).
             agg({'tweet_time':'nunique'}).
            sort_values('tweet_time',ascending=False))

        return self.number_of_weeks_account_posted
    
    
    def tweet_counts_by_account_and_week(self,top_IRA_accounts):
        def get_tweet_counts(df):
            return (df[df['user_screen_name'].isin(top_IRA_accounts)].
                    groupby(['tweet_time','user_screen_name'],as_index=False).
                     size().reset_index().sort_values(['user_screen_name','tweet_time']))

        self.tweet_counts = self.data.pipe(get_tweet_counts)

        return self.tweet_counts
    
    def save_data_for_bot_test(self,ta):
        def reformat_account_record_rtweet(df,ta):
            df=df[df['user_screen_name'] == ta]
            new_columns = df.columns.values
            for n,o in zip(self.rtweet_cols, self.ira_cols): new_columns[new_columns==o] = n
            df.columns=new_columns

            df=df.loc[:,self.rtweet_cols_fullset]
            return df

        directory = 'bot_detection/'
        if not os.path.exists(directory):
            os.makedirs(directory)

        self.data.pipe(reformat_account_record_rtweet,ta).to_csv(directory+ta)
        
    def get_top_accounts_with_botornot_score(self,fn,top_IRA_accounts):
        ## load probability each account is a bot
        botornot = pd.read_csv(fn,index_col=0).loc[top_IRA_accounts]
        top_IRA_accounts = botornot[~botornot.isnull().all(1)].index

        return top_IRA_accounts

    def create_wide_dataframe(self,tweet_counts):
    
        #create wide dataframe
        def widen_and_impute(df):
            df=df.pivot(index='tweet_time',columns='user_screen_name',values=0)

            #range of dates at weekly intervals
            periods =  ((df.index.max() - df.index.min())/7).days + 1
            idx = pd.date_range(df.index.min(), df.index.max(),periods =periods)

            #add missing weeks and replace missing values with zeros
            df=df.reindex(idx).fillna(0)

            return df.T

        return tweet_counts.pipe(widen_and_impute)
