import IRA_tweets
import pandas as pd
import matplotlib.pyplot as plt
import os

self = IRA_tweets.IRA_tweets()
self.english_only_tweets()
self.round_dates_to_weekstarting()

number_of_weeks_posted = self.get_number_of_weeks_account_posted()

#accounts that tweeted in 100 or more weeks
top_IRA_accounts = (number_of_weeks_posted[number_of_weeks_posted['tweet_time'] > 100]
                    ['user_screen_name'].values)

#get non-anonymized accounts (short names)
top_IRA_accounts = [w for w in top_IRA_accounts if len(w) < 20] 

# save log for high volume Twitter accounts in rtweet format
#for ta in top_IRA_accounts:#[1]
#    self.save_data_for_bot_test(ta)

fn = 'botornot.csv' # path to botornot probabilities
top_IRA_accounts = self.get_top_accounts_with_botornot_score(fn,top_IRA_accounts)

tweet_counts = self.tweet_counts_by_account_and_week(top_IRA_accounts)
df_ts = self.create_wide_dataframe(tweet_counts)

df_ts.to_csv('IRA_timeseries')



### plot and save rough pdfs of time series 
for name in top_IRA_accounts:

    x=tweet_counts[tweet_counts['user_screen_name'] == name]
    x.index=x['tweet_time']
    x=x[0]

    f,ax = plt.subplots()
    
    ax.plot(x,'o-')
    ax.set_title(name)
    
    plot_directory = 'timeseries_plots/'
    if not os.path.exists(plot_directory):
        os.makedirs(plot_directory)
        
    plt.savefig(plot_directory + name + '.pdf')
    
    plt.close()
