#########
### VERTICALS
#########

verticals_list <- list(
  cars = list(
    poland = list(
      properties = list(
        permanent = c('platform','event_type'),
        loggeduser = c('user_status','business_status','user_id'),
        categories = c('cat_l1_id','cat_l1_name','cat_l2_id','cat_l2_name'),
        location = c('region_id','region_name','city_id','city_name'),
        shortlisted = list(
          adpage = list(
            properties = c('touch_point_page','price_currency','poster_type','item_condition'),
            touch_point_page = c('ad_page'),
            price_currency = c('PLN','EUR','RON'),
            poster_type = c('business','private'),
            item_condition = c('new','used')
          ),
          listing = list(
            properties = c('touch_point_page','item_condition','search_history','listing_type','listing_format'),
            touch_point_page = c('listing','filters'),
            item_condition = c('all','new','used'),
            search_history = c('yes','no'),
            listing_type = c('browse','search'),
            listing_format = c('gallery','list','mosaic')
          ),
          posting = list(
            properties = c('touch_point_page','form_step'),
            touch_point_page = c('posting','editing','draft'),
            form_step = c('compulsory','additional')
          ),
          multipay = list(
            properties = c('touch_point_page','multipay_type'),
            touch_point_page = c('c2c','b2c'),
            multipay_type = c('features','packages_and_features')
          ),
          seller = list(
            properties = c('touch_point_page','listing_type','other_locations'),
            touch_point_page = c('seller_listing'),
            listing_type = c('browse','search'),
            other_locations = c('shown','not_shown')
          ),
          home = list(
            properties = c('touch_point_page'),
            touch_point_page = c('home')
          ),
          account = list(
            properties = c('touch_point_page'),
            touch_point_page = c('my_ads_pending','my_ads_closed','my_ads_active','my_ads_moderated','ad_detail','my_messages_received','my_messages_sent',
                                 'my_messages_archive','chat_all_conversations','chat_read_conversation','my_observed_ads','my_observed_searches')
          ),
          others = list(
            properties = c()
          ),
          promotedads = list(
            properties = c()
          )
        ) # end shortlisted
      ),
      db = list(
        categories = list(
          l1_cat = otomoto_cat1,
          l2_cat = otomoto_cat2,
          l1_parent_l2_id = unique(otomoto_cat2[['l2_id']]),
          brand = c(1,9,29,31,57,65,73),
          model = c(29,65,73),
          mileage = c(29,57,65,73),
          year = c(1,9,29,31,57,65,73),
          item_condition = c(1,9,29,31,57,65,73)
        ),
        location = list(
          regions = otomoto_regions,
          cities = otomoto_cities
        )
      )
    ), # end poland
    romania = list(
      properties = list(
        permanent = c('platform','event_type'),
        loggeduser = c('user_status','business_status','user_id'),
        categories = c('cat_l1_id','cat_l1_name','cat_l2_id','cat_l2_name'),
        location = c('region_id','region_name','city_id','city_name'),
        shortlisted = list(
          adpage = list(
            properties = c('touch_point_page','price_currency','poster_type','item_condition'),
            touch_point_page = c('ad_page'),
            price_currency = c('PLN','EUR','RON'),
            poster_type = c('business','private'),
            item_condition = c('new','used')
          ),
          listing = list(
            properties = c('touch_point_page','item_condition','search_history','listing_type','listing_format'),
            touch_point_page = c('listing','filters'),
            item_condition = c('all','new','used'),
            search_history = c('yes','no'),
            listing_type = c('browse','search'),
            listing_format = c('gallery','list','mosaic')
          ),
          posting = list(
            properties = c('touch_point_page','form_step'),
            touch_point_page = c('posting','editing','draft'),
            form_step = c('compulsory','additional')
          ),
          multipay = list(
            properties = c('touch_point_page','multipay_type'),
            touch_point_page = c('c2c','b2c'),
            multipay_type = c('features','packages_and_features')
          ),
          seller = list(
            properties = c('touch_point_page','listing_type','other_locations'),
            touch_point_page = c('seller_listing'),
            listing_type = c('browse','search'),
            other_locations = c('shown','not_shown')
          ),
          home = list(
            properties = c('touch_point_page'),
            touch_point_page = c('home')
          ),
          account = list(
            properties = c('touch_point_page'),
            touch_point_page = c('my_ads_pending','my_ads_closed','my_ads_active','my_ads_moderated','ad_detail','my_messages_received','my_messages_sent',
                                 'my_messages_archive','chat_all_conversations','chat_read_conversation','my_observed_ads','my_observed_searches')
          ),
          others = list(
            properties = c()
          ),
          promotedads = list(
            properties = c()
          )
        ) # end shortlisted
      ),
      db = list(
        categories = list(
          l1_cat = autovit_cat1,
          l2_cat = autovit_cat2,
          l1_parent_l2_id = unique(autovit_cat2[['l1_id']]),
          brand = c(1,9,29,31,57,65,81),
          model = c(29,65,81),
          mileage = c(29,57,65,81),
          year = c(1,9,29,31,57,65,81),
          item_condition = c(1,9,29,31,57,65,67,81)
        ),
        location = list(
          regions = autovit_regions,
          cities = autovit_cities
        )
      )
    ), # end romania
    portugal = list(
      properties = list(
        permanent = c('platform','event_type'),
        loggeduser = c('user_status','business_status','user_id'),
        categories = c('cat_l1_id','cat_l1_name','cat_l2_id','cat_l2_name'),
        location = c('region_id','region_name','city_id','city_name'),
        shortlisted = list(
          adpage = list(
            properties = c('touch_point_page','price_currency','poster_type','item_condition'),
            touch_point_page = c('ad_page'),
            price_currency = c('PLN','EUR','RON'),
            poster_type = c('business','private'),
            item_condition = c('new','used')
          ),
          listing = list(
            properties = c('touch_point_page','item_condition','search_history','listing_type','listing_format'),
            touch_point_page = c('listing','filters'),
            item_condition = c('all','new','used'),
            search_history = c('yes','no'),
            listing_type = c('browse','search'),
            listing_format = c('gallery','list','mosaic')
          ),
          posting = list(
            properties = c('touch_point_page','form_step'),
            touch_point_page = c('posting','editing','draft'),
            form_step = c('compulsory','additional')
          ),
          multipay = list(
            properties = c('touch_point_page','multipay_type'),
            touch_point_page = c('c2c','b2c'),
            multipay_type = c('features','packages_and_features')
          ),
          seller = list(
            properties = c('touch_point_page','listing_type','other_locations'),
            touch_point_page = c('seller_listing'),
            listing_type = c('browse','search'),
            other_locations = c('shown','not_shown')
          ),
          home = list(
            properties = c('touch_point_page'),
            touch_point_page = c('home')
          ),
          account = list(
            properties = c('touch_point_page'),
            touch_point_page = c('my_ads_pending','my_ads_closed','my_ads_active','my_ads_moderated','ad_detail','my_messages_received','my_messages_sent',
                                 'my_messages_archive','chat_all_conversations','chat_read_conversation','my_observed_ads','my_observed_searches')
          ),
          others = list(
            properties = c()
          ),
          promotedads = list(
            properties = c()
          )
        ) # end shortlisted
      ),
      db = list(
        categories = list(
          l1_cat = standvirtual_cat1,
          brand = c(29,57,65,73,659,660),
          model = c(29,57,65,73,659,660),
          mileage = c(29,65,73),
          year = c(29,57,65,73,659,660),
          item_condition = c(29,57,65,73,659,660,661)
        ),
        location = list(
          regions = standvirtual_regions,
          cities = standvirtual_cities
        )
      )
    ) # end portugal
  ), # end cars
  
  realestate = list(
    poland = list(
      properties = list(
        permanent = c('platform','event_type'),
        loggeduser = c('user_status','business_status','user_id'),
        categories = c('cat_l1_id','cat_l1_name','cat_l2_id','cat_l2_name'),
        location = c('region_id','region_name','city_id','city_name'),
        shortlisted = list(
          adpage = list(
            properties = c('touch_point_page','business','price_currency','poster_type'),
            touch_point_page = c('ad_page'),
            business = c('holidays','rent','sell','vacation'),
            price_currency = c('PLN','EUR','RON'),
            poster_type = c('business','private')
          ),
          listing = list(
            properties = c('touch_point_page','business','only_open_day','only_private','with_photo','listing_type','listing_format','drawing_used'),
            touch_point_page = c('listing'),
            business = c('holidays','rent','sell','vacation'),
            only_open_day = c('yes','no'),
            only_private = c('yes','no'),
            with_photo = c('yes','no'),
            listing_type = c('browse','listing','search','static'),
            listing_format = c('gallery','list','map'),
            drawing_used = c('yes','no')
          ),
          posting = list(
            properties = c('touch_point_page','business'),
            touch_point_page = c('posting','editing'),
            business = c('holidays','rent','sell','vacation')
          ),
          multipay = list(
            properties = c('touch_point_page','multipay_type'),
            touch_point_page = c('c2c','b2c'),
            multipay_type = c('features','packages_and_features')
          ),
          seller = list(
            properties = c('touch_point_page','listing_type'),
            touch_point_page = c('agencies_listing','developers_listing','seller_listing','developers_offers','developers_offers_listing'),
            listing_type = c('browse','listing','search','static')
          ),
          home = list(
            properties = c('touch_point_page','business'),
            touch_point_page = c('home'),
            business = c('holidays','rent','sell','vacation')
          ),
          account = list(
            properties = c('touch_point_page'),
            touch_point_page = c('my_ads_pending','my_ads_closed','my_ads_active','my_messages_received','my_messages_sent','my_messages_archive',
                                 'my_observed_ads','my_observed_searches','seen_ads_listing')
          ),
          others = list(
            properties = c()
          ),
          multipayb2c = list(
            properties = c('touch_point_page','multipay_type','f_payment_method'),
            touch_point_page = c('b2c'),
            multipay_type = c('account_packages'),
            f_payment_method = c('postpay')
          )
        ) # end shortlisted
      ), # end properties
      db = list(
        categories = list(
          l1_cat = otodompl_cat1,
          rooms = c(101,102,201,202)
        ),
        location = list(
          regions = otodompl_regions,
          cities = otodompl_cities
        )
      )
    ), # end poland
    romania = list(
      properties = list(
        permanent = c('platform','event_type'),
        loggeduser = c('user_status','business_status','user_id'),
        categories = c('cat_l1_id','cat_l1_name','cat_l2_id','cat_l2_name'),
        location = c('region_id','region_name','city_id','city_name'),
        shortlisted = list(
          adpage = list(
            properties = c('touch_point_page','business','price_currency','poster_type'),
            touch_point_page = c('ad_page'),
            business = c('holidays','rent','sell','vacation'),
            price_currency = c('PLN','EUR','RON'),
            poster_type = c('business','private')
          ),
          listing = list(
            properties = c('touch_point_page','business','only_open_day','only_private','with_photo','listing_type','listing_format','drawing_used'),
            touch_point_page = c('listing'),
            business = c('holidays','rent','sell','vacation'),
            only_open_day = c('yes','no'),
            only_private = c('yes','no'),
            with_photo = c('yes','no'),
            listing_type = c('browse','listing','search','static'),
            listing_format = c('gallery','list','map'),
            drawing_used = c('yes','no')
          ),
          posting = list(
            properties = c('touch_point_page','business'),
            touch_point_page = c('posting','editing'),
            business = c('holidays','rent','sell','vacation')
          ),
          multipay = list(
            properties = c('touch_point_page','multipay_type'),
            touch_point_page = c('c2c','b2c'),
            multipay_type = c('features','packages_and_features')
          ),
          seller = list(
            properties = c('touch_point_page','listing_type'),
            touch_point_page = c('agencies_listing','developers_listing','seller_listing','developers_offers','developers_offers_listing'),
            listing_type = c('browse','listing','search','static')
          ),
          home = list(
            properties = c('touch_point_page','business'),
            touch_point_page = c('home'),
            business = c('holidays','rent','sell','vacation')
          ),
          account = list(
            properties = c('touch_point_page'),
            touch_point_page = c('my_ads_pending','my_ads_closed','my_ads_active','my_messages_received','my_messages_sent','my_messages_archive',
                                 'my_observed_ads','my_observed_searches','seen_ads_listing')
          ),
          others = list(
            properties = c()
          ),
          multipayb2c = list(
            properties = c('touch_point_page','multipay_type','f_payment_method'),
            touch_point_page = c('b2c'),
            multipay_type = c('account_packages'),
            f_payment_method = c('postpay')
          )
        ) # end shortlisted
      ), # end properties
      db = list(
        categories = list(
          l1_cat = storiaro_cat1,
          rooms = c(101,102,201,202)
        ),
        location = list(
          regions = storiaro_regions,
          cities = storiaro_cities
        )
      )
    ), # end romania
    portugal = list(
      properties = list(
        permanent = c('platform','event_type'),
        loggeduser = c('user_status','business_status','user_id'),
        categories = c('cat_l1_id','cat_l1_name','cat_l2_id','cat_l2_name'),
        location = c('region_id','region_name','city_id','city_name'),
        shortlisted = list(
          adpage = list(
            properties = c('touch_point_page','business','price_currency','poster_type'),
            touch_point_page = c('ad_page'),
            business = c('holidays','rent','sell','vacation'),
            price_currency = c('PLN','EUR','RON'),
            poster_type = c('business','private')
          ),
          listing = list(
            properties = c('touch_point_page','business','only_open_day','only_private','with_photo','listing_type','listing_format','drawing_used'),
            touch_point_page = c('listing'),
            business = c('holidays','rent','sell','vacation'),
            only_open_day = c('yes','no'),
            only_private = c('yes','no'),
            with_photo = c('yes','no'),
            listing_type = c('browse','listing','search','static'),
            listing_format = c('gallery','list','map'),
            drawing_used = c('yes','no')
          ),
          posting = list(
            properties = c('touch_point_page','business'),
            touch_point_page = c('posting','editing'),
            business = c('holidays','rent','sell','vacation')
          ),
          multipay = list(
            properties = c('touch_point_page','multipay_type'),
            touch_point_page = c('c2c','b2c'),
            multipay_type = c('features','packages_and_features')
          ),
          seller = list(
            properties = c('touch_point_page','listing_type'),
            touch_point_page = c('agencies_listing','developers_listing','seller_listing','developers_offers','developers_offers_listing'),
            listing_type = c('browse','listing','search','static')
          ),
          home = list(
            properties = c('touch_point_page','business'),
            touch_point_page = c('home'),
            business = c('holidays','rent','sell','vacation')
          ),
          account = list(
            properties = c('touch_point_page'),
            touch_point_page = c('my_ads_pending','my_ads_closed','my_ads_active','my_messages_received','my_messages_sent','my_messages_archive',
                                 'my_observed_ads','my_observed_searches','seen_ads_listing')
          ),
          others = list(
            properties = c()
          ),
          multipayb2c = list(
            properties = c('touch_point_page','multipay_type','f_payment_method'),
            touch_point_page = c('b2c'),
            multipay_type = c('account_packages'),
            f_payment_method = c('postpay')
          )
        ) # end shortlisted
      ), # end properties
      db = list(
        categories = list(
          l1_cat = imovirtual_cat1,
          rooms = c(101,102,201,202,203)
        ),
        locatin = list(
          regions = imovirtual_regions,
          cities = imovirtual_cities
        )
      )
    ) # end portugal
  ) # end real estate
) # verticals_list
