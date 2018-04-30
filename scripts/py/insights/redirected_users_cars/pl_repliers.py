# import libraries
import pandas as pd
import psycopg2
import numpy as np

con = psycopg2.connect(
        dbname='DB_NAME',
        host='HOST NAME',
        port='PORT',
        user='USER NAME',
        password='PW'
        )

cur = con.cursor()

# dates for the loop
datesList = [
        '2017-12-01',
        '2017-12-02',
        '2017-12-03',
        '2017-12-04',
        '2017-12-05',
        '2017-12-06',
        '2017-12-07']

# replies list
replies_olx = ['reply_phone_1step','reply_chat_sent']
replies_otomoto = ['reply_phone_show','reply_message_click','reply_phone_sms','reply_phone_call']

# create the empty df that we'll use for Tableau
tableUsers = pd.DataFrame()

# loop starts
for date in datesList:
    query = "select * from stream_pl_repliers where event_date = '" + date + "' order by cd_cookie,event_ts asc"
    cur.execute(query)
    datasetDay =np.array(cur.fetchall())
    
    # create dictionnaries
    users = {}
    adArrays = {}
    
    # setup the Json
    for i in datasetDay:
        users[i[1]] = {
                # segments
                'did_olx':0, #
                'did_otomoto':0, #
                'platform':None, #
                'first_exp':None, #
                'viewed_ad_olx':0, #
                'viewed_ad_otomoto':0, #
                'viewed_ad_otomoto_via_olx':0, #
                'browsed_olx':0, #
                'browsed_otomoto':0, #
                'replied_otomoto':0, #
                'replied_otomoto_via_olx':0, #
                'replied_olx':0, #
                'faved_olx':0, #
                'faved_otomoto':0, #
                'faved_otomoto_via_olx':0, #
                
                # status
                'tp_status':0, #
                
                # raw
                'replies_otomoto':0, #
                'replies_otomoto_via_olx':0, #
                'replies_olx':0, #
                'adviews_otomoto':0, #
                'adviews_otomoto_via_olx':0, #
                'adviews_olx':0, #
                'searches_otomoto':0, #
                'searches_olx':0, #
                'favs_otomoto':0, #
                'favs_otomoto_via_olx':0, #
                'favs_olx':0, #
                'touch_points':0, #
                
                # before first reply
                'adviews_otomoto_br':0,
                'adviews_otomoto_via_olx_br':0,
                'adviews_olx_br':0,
                'replies_olx_br':0,
                'searches_otomoto_br':0,
                'searches_olx_br':0,
                'touch_points_br':0
                }
        
        adArrays[i[1]] = {
                # array of ads
                'ad_views_olx':[], #
                'ad_views_otomoto':[], #
                'replies_olx':[], #
                'replies_otomoto':[], #
                'favs_olx':[], #
                'favs_otomoto':[] #
                }                   
    
    
    # fill the Json
    for i in datasetDay:
        
        # get the first row of the user's day
        if users[i[1]]['first_exp'] == None:
            if i[0] == 'otomoto' and i[7] == 'olx':
                users[i[1]]['first_exp'] = 'olx'
            else:
                users[i[1]]['first_exp'] = i[0]
            
        # get the platform (based on the first event)
        if users[i[1]]['platform'] == None:
            users[i[1]]['platform'] = i[2]
        
        # in OLX
        if i[0] == 'olx':
            
            # then he did OLX
            users[i[1]]['did_olx'] = 1
            
            # adviews
            if i[5] == 'ad_page' and i[6] not in adArrays[i[1]]['ad_views_olx']:
                adArrays[i[1]]['ad_views_olx'].append(i[6])
                users[i[1]]['adviews_olx'] += 1
                users[i[1]]['viewed_ad_olx'] = 1
            
            # search
            if i[5] == 'listing':
                users[i[1]]['searches_olx'] += 1
                users[i[1]]['browsed_olx'] = 1
            
            # replies
            if i[5] in replies_olx and i[6] not in adArrays[i[1]]['replies_olx']:
                adArrays[i[1]]['replies_olx'].append(i[6])
                users[i[1]]['replies_olx'] += 1
                users[i[1]]['replied_olx'] = 1
            
            # favs
            if i[5] == 'favourite_ad_click' and i[6] not in adArrays[i[1]]['favs_olx']:    
                adArrays[i[1]]['favs_olx'].append(i[6])
                users[i[1]]['favs_olx'] += 1
                users[i[1]]['faved_olx'] = 1
    
            # touch points
            if users[i[1]]['tp_status'] == 1:
                users[i[1]]['tp_status'] = 0
                users[i[1]]['touch_points'] += 1
                
        # in Otomoto
        if i[0] == 'otomoto':
            
            # then he did Otomoto
            users[i[1]]['did_otomoto'] = 1
            
            # if he comes from OLX, I update the tp_status to 1
            if i[7] == 'olx':
                users[i[1]]['tp_status'] = 1
            
            # adviews
            if i[5] == 'ad_page' and i[6] not in adArrays[i[1]]['ad_views_otomoto']:
                adArrays[i[1]]['ad_views_otomoto'].append(i[6])
                users[i[1]]['adviews_otomoto'] += 1
                users[i[1]]['viewed_ad_otomoto'] = 1
                if i[7] == 'olx':
                    users[i[1]]['adviews_otomoto_via_olx'] += 1
                    users[i[1]]['viewed_ad_otomoto_via_olx'] = 1
    
            # search
            if i[5] == 'listing':
                users[i[1]]['searches_otomoto'] += 1
                users[i[1]]['browsed_otomoto'] = 1
            
            # replies
            if i[5] in replies_otomoto and i[6] not in adArrays[i[1]]['replies_otomoto']:
                if users[i[1]]['replies_otomoto'] == 0:
                    users[i[1]]['adviews_olx_br'] = users[i[1]]['adviews_olx']
                    users[i[1]]['replies_olx_br'] = users[i[1]]['replies_olx']
                    users[i[1]]['searches_olx_br'] = users[i[1]]['searches_olx']
                    users[i[1]]['adviews_otomoto_br'] = users[i[1]]['adviews_otomoto']
                    users[i[1]]['adviews_otomoto_via_olx_br'] = users[i[1]]['adviews_otomoto_via_olx']
                    users[i[1]]['searches_otomoto_br'] = users[i[1]]['searches_otomoto']
                    users[i[1]]['touch_points_br'] = users[i[1]]['touch_points']
                adArrays[i[1]]['replies_otomoto'].append(i[6])
                users[i[1]]['replies_otomoto'] += 1
                users[i[1]]['replied_otomoto'] = 1
                if i[7] == 'olx':
                    users[i[1]]['replies_otomoto_via_olx'] += 1
                    users[i[1]]['replied_otomoto_via_olx'] = 1
            
            # favs
            if i[5] == 'favourite_ad_click' and i[6] not in adArrays[i[1]]['favs_otomoto']:
                adArrays[i[1]]['favs_otomoto'].append(i[6])
                users[i[1]]['favs_otomoto'] += 1
                users[i[1]]['faved_otomoto'] = 1
                if i[7] == 'olx':
                    users[i[1]]['favs_otomoto_via_olx'] += 1
                    users[i[1]]['faved_otomoto_via_olx'] = 1        
            
            
            
    # transform the Json user into a tmp data frame
    tmpDataFrame =  pd.DataFrame.from_dict({(j):users[j]
            for j in users.keys()}, orient = 'index')
    
    # add the date
    tmpDataFrame['date'] = date
    
    # rbind the dataset for this day in the global DF
    tableUsers = tableUsers.append(tmpDataFrame)

# loop ends       
        
                
# extract to CSV            
tableUsers.to_csv('pl_repliers.csv')
                
#users['15b5d33ebf3xe54c189f                ']


# close connexion
cur.close() 
con.close()
