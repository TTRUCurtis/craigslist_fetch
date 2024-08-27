import requests
import math
import datetime
import numpy as np
import pandas as pd
from pathlib import Path
import sqlalchemy as db
import getopt
import sys
from bs4 import BeautifulSoup
import json
import time
import html


def main():
    options = {}
    options["debug_level"] = 0
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["database"] = "douglasvbellew"
    options["city_name"] = "losangeles"
    options["category_name"] = "ppa"
    options["replace_database_tables"] = True
    options["table_append_timestamp"] = False

    name_to_category_converter = {"antiques":"ata",
                                  "appliances":"ppa",
                                  "arts+crafts":"ara",
                                  "atvs/utvs/snow":"sna",
                                  "auto parts":"pta",
                                  "auto wheels & tires":"wta",
                                  "aviation":"ava",
                                  "baby+kids":"bba",
                                  "barter":"bar",
                                  "beauty+hlth":"haa",
                                  "bike parts":"bip",
                                  "bikes":"bia",
                                  "boat parts":"bpa",
                                  "boats":"boo",
                                  "books":"bka",
                                  "business":"bfa",
                                  "cars+trucks":"cta",
                                  "cds/dvd/vhs":"ema",
                                  "cell phones":"moa",
                                  "clothes+acc":"cla",
                                  "collectables":"cba",
                                  "computer parts":"syp",
                                  "computers":"sya",
                                  "electronics":"ela",
                                  "farm+garden":"gra",
                                  "free stuff":"zip",
                                  "furniture":"fua",
                                  "garage sales":"gms",
                                  "general":"foa",
                                  "heavy equipment":"hva",
                                  "household":"hsa",
                                  "jewelry":"jwa",
                                  "materials":"maa",
                                  "motorcycle parts":"mpa",
                                  "motorcycles":"mca",
                                  "music instr":"msa",
                                  "photo+video":"pha",
                                  "RVs":"rva",
                                  "sporting":"sga",
                                  "tickets":"tia",
                                  "tools":"tla",
                                  "toys+games":"taa",
                                  "trailers":"tra",
                                  "video gaming":"vga",
                                  "wanted":"waa"
                                  }
    category_to_url_converter = {"ata":"atq","ppa":"app","ara":"art","sna":"snw",
                                 "pta":"pts","wta":"wto","ava":"avo","bba":"bab",
                                 "bar":"bar","haa":"hab","bip":"bop","bia":"bik",
                                 "bpa":"bpo","boo":"boa","bka":"bks","bfa":"bfs",
                                 "cta":"ctd","ema":"emd","moa":"mob","cla":"clo",
                                 "cba":"clt","syp":"sop","sya":"sys","ela":"ele",
                                 "gra":"grd","zip":"zip","fua":"fuo","gms":"gms",
                                 "foa":"for","hva":"hvo","hsa":"hsh","jwa":"jwl",
                                 "maa":"mat","mpa":"mpo","mca":"mcd","msa":"msg",
                                 "pha":"pho","rva":"rvs","sga":"spo","tia":"tid",
                                 "tla":"tls","taa":"tag","tra":"tro","vga":"vgm",
                                 "waa":"wan"
                                 } 

#Category Reference            
#            [["bbb", "services", "", "B", [4, "biz", "small biz ads", ""], [76, "cps", "computer services", ""], [77, "crs", "creative services", ""], [79, "evs", "event services", ""], [80, "hss", "household services", ""], [81, "lss", "lessons & tutoring", ""], [82, "lbs", "labor & moving", "labor / hauling / moving"], [83, "sks", "skilled trade services", ""], [103, "lgs", "legal services", ""], [104, "fns", "financial services", ""], [105, "rts", "real estate services", ""], [106, "aos", "automotive services", ""], [138, "bts", "beauty services", ""], [139, "wet", "write/edit/trans", "writing / editing / translation"], [140, "trv", "travel/vacation", "travel/vacation services"], [154, "fgs", "farm & garden services", ""], [155, "pas", "pet services", ""], [156, "mas", "marine services", ""], [158, "cys", "cycle services", ""], [207, "cms", "cell phone / mobile services", ""], [210, "hws", "health/wellness", "health/wellness services"]], ["ccc", "community", "", "C", [3, "com", "general", "general community"], [29, "vol", "volunteers", ""], [35, "act", "activity partners", ""], [36, "rid", "rideshare", ""], [37, "pet", "pets", ""], [56, "kid", "childcare", ""], [63, "mis", "missed connections", ""], [70, "ats", "artists", ""], [71, "muc", "musicians", ""], [87, "pol", "politics", ""], [88, "laf", "lost & found", ""], [90, "rnr", "rants & raves", ""], [91, "grp", "groups", ""], [116, "vnn", "local news and views", ""]], ["eee", "events", "events/classes", "E", [8, "eve", "events", ""], [38, "cls", "classes", ""]], ["ggg", "gigs", "", "G", [108, "tlg", "talent gigs", ""], [109, "cwg", "crew gigs", ""], [110, "cpg", "computer gigs", ""], [111, "lbg", "labor gigs", ""], [112, "wrg", "writing gigs", ""], [113, "dmg", "domestic gigs", ""], [114, "crg", "creative gigs", ""], [115, "evg", "event gigs", ""]], ["hhh", "housing", "", "H", [0, "rea", "real estate for sale", "real estate", [143, "reo", "real estate - by owner", ""], [144, "reb", "real estate - by broker", ""]], [1, "apa", "apartments / housing for rent", ""], [2, "hou", "wanted: apts", ""], [18, "roo", "rooms & shares", ""], [19, "sha", "wanted: room/share", ""], [39, "sub", "sublets & temporary", ""], [40, "off", "office & commercial", ""], [41, "prk", "parking & storage", ""], [58, "sbw", "wanted: sublet/temp", ""], [65, "swp", "housing swap", ""], [99, "vac", "vacation rentals", ""], [121, "rew", "wanted: real estate", ""]], ["jjj", "jobs", "", "J", [11, "web", "web design", "web/html/info design"], [12, "bus", "business", "business/mgmt"], [13, "mar", "marketing", "marketing/advertising/pr"], [15, "etc", "etcetera", "et cetera"], [16, "wri", "writing", "writing/editing"], [21, "sof", "software", "software/qa/dba/etc"], [23, "acc", "finance", "accounting/finance"], [24, "ofc", "admin/office", ""], [25, "med", "media", "art/media/design"], [26, "hea", "healthcare", ""], [27, "ret", "retail/wholesale", ""], [28, "npo", "nonprofit", ""], [47, "lgl", "legal", "legal/paralegal"], [48, "egr", "engineering", "architect/engineer/cad"], [49, "sls", "sales", ""], [50, "sad", "systems/networking", ""], [52, "tfr", "tv video radio", "tv/film/video/radio"], [54, "hum", "human resource", ""], [55, "tch", "tech support", "technical support"], [57, "edu", "education", "education/teaching"], [59, "trd", "skilled trades", "skilled trades/artisan"], [61, "gov", "government", ""], [75, "sci", "science", "science/biotech"], [100, "csr", "customer service", ""], [125, "trp", "transport", "transportation"], [126, "spa", "salon/spa/fitness", ""], [127, "rej", "real estate", ""], [128, "mnu", "manufacturing", ""], [129, "fbh", "food/bev/hosp", "food/beverage/hospitality"], [130, "lab", "general labor", ""], [131, "sec", "security", ""]], ["rrr", "resumes", "", "R", [10, "res", "resumes", "resumes / job wanted"]], ["sss", "for sale", "", "S", [0, "foa", "general", "general for sale", [5, "for", "general for sale - by owner", ""], [179, "fod", "general for sale - by dealer", ""]], [0, "sya", "computers", "", [7, "sys", "computers - by owner", ""], [166, "syd", "computers - by dealer", ""]], [0, "waa", "wanted", "", [20, "wan", "wanted - by owner", ""], [190, "wad", "wanted - by dealer", ""]], [0, "tia", "tickets", "", [44, "tix", "tickets - by owner", ""], [161, "tid", "tickets - by dealer", ""]], [0, "bia", "bikes", "bicycles", [68, "bik", "bicycles - by owner", ""], [172, "bid", "bicycles - by dealer", ""]], [0, "mca", "motorcycles", "motorcycles/scooters", [69, "mcy", "motorcycles", "motorcycles/scooters - by owner"], [160, "mcd", "motorcycles", "motorcycles/scooters - by dealer"]], [0, "bka", "books", "books & magazines", [92, "bks", "books", "books & magazines - by owner"], [173, "bkd", "books", "books & magazines - by dealer"]], [0, "sga", "sporting", "sporting goods", [93, "spo", "sporting goods - by owner", ""], [186, "sgd", "sporting goods - by dealer", ""]], [0, "cla", "clothes+acc", "clothing & accessories", [94, "clo", "clothes+acc", "clothing & accessories - by owner"], [176, "cld", "clothes+acc", "clothing & accessories - by dealer"]], [0, "cba", "collectibles", "", [95, "clt", "collectibles - by owner", ""], [177, "cbd", "collectibles - by dealer", ""]], [0, "ela", "electronics", "", [96, "ele", "electronics - by owner", ""], [167, "eld", "electronics - by dealer", ""]], [0, "hsa", "household", "household items", [97, "hsh", "household items - by owner", ""], [181, "hsd", "household items - by dealer", ""]], [0, "msa", "music instr", "musical instruments", [98, "msg", "musical instruments - by owner", ""], [184, "msd", "musical instruments - by dealer", ""]], [0, "baa", "baby+kids", "baby & kid stuff", [107, "bab", "baby & kid stuff - by owner", ""], [171, "bad", "baby & kid stuff - by dealer", ""]], [0, "ema", "cds/dvd/vhs", "cds / dvds / vhs", [117, "emd", "cds / dvds / vhs - by owner", ""], [175, "emq", "cds / dvds / vhs - by dealer", ""]], [0, "tla", "tools", "", [118, "tls", "tools - by owner", ""], [187, "tld", "tools - by dealer", ""]], [0, "boo", "boats", "", [119, "boa", "boats - by owner", ""], [164, "bod", "boats - by dealer", ""]], [0, "jwa", "jewelry", "", [120, "jwl", "jewelry - by owner", ""], [182, "jwd", "jewelry - by dealer", ""]], [0, "pta", "auto parts", "", [122, "pts", "auto parts - by owner", ""], [163, "ptd", "auto parts - by dealer", ""]], [0, "rva", "RVs", "recreational vehicles", [124, "rvs", "rvs - by owner", ""], [168, "rvd", "rvs - by dealer", ""]], [0, "taa", "toys+games", "toys & games", [132, "tag", "toys+games", "toys & games - by owner"], [188, "tad", "toys+games", "toys & games - by dealer"]], [0, "gra", "farm+garden", "farm & garden", [133, "grd", "farm+garden", "farm & garden - by owner"], [178, "grq", "farm+garden", "farm & garden - by dealer"]], [0, "bfa", "business", "", [134, "bfs", "business/commercial - by owner", ""], [174, "bfd", "business/commercial - by dealer", ""]], [0, "ara", "arts+crafts", "arts & crafts", [135, "art", "arts & crafts - by owner", ""], [170, "ard", "arts & crafts - by dealer", ""]], [0, "maa", "materials", "", [136, "mat", "materials - by owner", ""], [183, "mad", "materials - by dealer", ""]], [0, "pha", "photo+video", "photo/video", [137, "pho", "photo/video - by owner", ""], [185, "phd", "photo/video - by dealer", ""]], [0, "fua", "furniture", "", [141, "fuo", "furniture - by owner", ""], [142, "fud", "furniture - by dealer", ""]], [0, "cta", "cars+trucks", "cars & trucks", [145, "cto", "cars+trucks", "cars & trucks - by owner"], [146, "ctd", "cars+trucks", "cars & trucks - by dealer"]], [0, "ppa", "appliances", "", [149, "app", "appliances - by owner", ""], [162, "ppd", "appliances - by dealer", ""]], [0, "ata", "antiques", "", [150, "atq", "antiques - by owner", ""], [169, "atd", "antiques - by dealer", ""]], [0, "vga", "video gaming", "", [151, "vgm", "video gaming - by owner", ""], [189, "vgd", "video gaming - by dealer", ""]], [0, "haa", "beauty+hlth", "health and beauty", [152, "hab", "health and beauty - by owner", ""], [180, "had", "health and beauty - by dealer", ""]], [0, "moa", "cell phones", "", [153, "mob", "cell phones - by owner", ""], [165, "mod", "cell phones - by dealer", ""]], [0, "sna", "atvs/utvs/snow", "atvs, utvs, snowmobiles", [191, "snw", "atvs, utvs, snowmobiles - by owner", ""], [192, "snd", "atvs, utvs, snowmobiles - by dealer", ""]], [0, "hva", "heavy equipment", "", [193, "hvo", "heavy equipment - by owner", ""], [194, "hvd", "heavy equipment - by dealer", ""]], [0, "mpa", "motorcycle parts", "motorcycle parts & accessories", [195, "mpo", "motorcycle parts - by owner", ""], [196, "mpd", "motorcycle parts - by dealer", ""]], [0, "bip", "bike parts", "bicycle parts", [197, "bop", "bicycle parts - by owner", ""], [198, "bdp", "bicycle parts - by dealer", ""]], [0, "syp", "computer parts", "", [199, "sop", "computer parts - by owner", ""], [200, "sdp", "computer parts - by dealer", ""]], [0, "bpa", "boat parts", "boat parts & accessories", [201, "bpo", "boat parts - by owner", ""], [202, "bpd", "boat parts - by dealer", ""]], [0, "wta", "auto wheels & tires", "", [203, "wto", "wheels+tires", "auto wheels & tires - by owner"], [204, "wtd", "wheels+tires", "auto wheels & tires - by dealer"]], [0, "tra", "trailers", "", [205, "tro", "trailers - by owner", ""], [206, "trb", "trailers - by dealer", ""]], [0, "ava", "aviation", "", [208, "avo", "aviation - by owner", ""], [209, "avd", "aviation - by dealer", ""]], [42, "bar", "barter", ""], [73, "gms", "garage sales", "garage & moving sales"], [101, "zip", "free stuff", ""]]].forEach((t=>{

    url_service_list = [[162, "ppd", "appliances - by dealer", ""],
                        [149, "app", "appliances - by owner", ""]]
                         
 
    df_columns = {"item_id":pd.Series(dtype='str'), 
                "item_id_int":pd.Series(dtype='int'), 
                "item_url":pd.Series(dtype='str'), 
                "city_retrieve":pd.Series(dtype='str'), 
                "category_retrieve" :pd.Series(dtype='str'), 
                "craigslist_location":pd.Series(dtype='str'), 
                "craigslist_location_description":pd.Series(dtype='str'),
                "craigslist_item_lat":pd.Series(dtype='int'), 
                "craigslist_item_long":pd.Series(dtype='int'),
                "item_name":pd.Series(dtype='str'), 
                "item_price":pd.Series(dtype='str'),
                "posted_date_epoch_utc":pd.Series(dtype='int'),
                "posted_date_timestamp_utc":pd.Series(dtype="datetime64[ns]"),
                "gathered_timestamp_utc":pd.Series(dtype="datetime64[ns]"), 
                "fetched":pd.Series(dtype='bool')}

    df_html_columns = {"url":pd.Series(dtype='str'), 
                        "city":pd.Series(dtype='str'), 
                        "area_id":pd.Series(dtype='int'),
                        "category":pd.Series(dtype='str'), 
                        "type":pd.Series(dtype='str'), 
                        "order" :pd.Series(dtype='str'),
                        "response_text":pd.Series(dtype='str'),
                        "gathered_timestamp_utc":pd.Series(dtype="datetime64[ns]")}

    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", ["city=", "category=", "database="])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    for option_tuple in optlist:
        if (option_tuple[0] == "--city"):
            options["city_name"] = option_tuple[1]
        elif (option_tuple[0] == "--category"):
            options["category_name"] = option_tuple[1]
        elif (option_tuple[0] == "--database"):
            options["database"] = option_tuple[1]
        elif (option_tuple[0] == "--replace_tables"):
            options["replace_database_tables"] = True
        elif (option_tuple[0] == "--table_append_timestamp"):
            options["table_append_timestamp"] = True

    item_count_per_fetch = 1080  #This is a website magic number.  Don't change

    # TODO: START LOOP HERE (LOOP OVER CITY/CATEGORY) .. make sure to put in some form of sleep between each get so you don't
    # get tagged by the server.  (20 secs per category?) 

    item_categories = [options["category_name"]]

    if (item_categories[0] == "all") :
        item_categories = name_to_category_converter.values()

    for category in item_categories:

        print("FETCHING CITY:"+options["city_name"]+":CATEGORY:"+options["category_name"])
        print(datetime.datetime.now())
        options["category_name"] = category
        item_returns = []
        failed = False

        # Get the base URL.  We need this to get the area_id. (also gives data for the first 360 entries)
        url_string = "https://"+options["city_name"]+".craigslist.org/search/"+options["category_name"]+"#search=1~gallery~0~0"
        print(url_string)

        base_response = requests.get(url_string)
        response_code = base_response.status_code

        soup = BeautifulSoup(base_response.content, "html.parser")
        #Default to > 360 entries

        pulled_extra_entries = False
        scripts = soup.find_all("script")

        #This call is really fragile (especialy the "nearbyAreas" split.  Will need to think how to make better.)
        #Cutting this information out of a js function call.  Icky, but we need the area_id in order to pull from the
        #correct city
        for script in scripts:
            script_text = script.text
            if "window.cl.init" in script_text:
                json_text = script_text.split("\'location\':")[1].split("\'nearbyAreas\':")[0].rstrip()[:-1]
                json_obj = json.loads(json_text)
                area_id = str(json_obj["areaId"])
#            if (script.has_attr("id")):    
#                if (script["id"] == "ld_searchpage_results"):
#                    html_search_results = json.loads(script_text)


        html_df = pd.DataFrame(columns=df_html_columns)
        if (response_code >= 200 and response_code <= 299):

            #Successfully got the initial page.  Get the data from the api
            current_secs = math.floor((datetime.datetime.now(datetime.timezone.utc) - datetime.datetime(1970,1,1,0,0,2, tzinfo=datetime.timezone.utc)).total_seconds())
            url_string = "https://sapi.craigslist.org/web/v8/postings/search/full?batch="+area_id+"-"+str(current_secs)+"-0-1-0&cc=US&lang=en&searchPath="+options["category_name"]
            print(url_string)

            html_item = {}
            html_item["url"] = html.escape(url_string)
            html_item["city"] = options["city_name"], 
            html_item["area_id"] = area_id,
            html_item["category"] = options["category_name"], 
            html_item["type"] = "HTML", 
            html_item["order"] =  0,
            html_item["response_text"] = base_response.content,
            html_item["gathered_timestamp_utc"] = current_secs

            new_df  = pd.DataFrame(html_item, columns = df_html_columns, index=[0])
            if (len(html_df) > 0):
                html_df = pd.concat([html_df,new_df],ignore_index=True)
            else:
                html_df = new_df

            response = requests.get(url_string) 
            response_code = response.status_code 



            resp_data = {}
            if (response_code >= 200 and response_code <= 299):
                #Got API Data if contains "cacheId" that means we have more than 360 items, so will need to make further API calls to get data
                #For the remaining items.  If not, it means we have less than 360 items and will need to parse the initial HTML and get data
                #from that instead.
                resp_data = response.json() 

                api_item = {}
                api_item["url"] = html.escape(url_string)
                api_item["city"] = options["city_name"], 
                api_item["area_id"] = area_id,
                api_item["category"] = options["category_name"], 
                api_item["type"] = "SINGLE_API", 
                api_item["order"] =  0,
                api_item["response_text"] = json.dumps(resp_data),
                api_item["gathered_timestamp_utc"] = current_secs

                new_df  = pd.DataFrame(api_item, columns = df_html_columns, index=[0])
                if (len(html_df) > 0):
                    html_df = pd.concat([html_df,new_df],ignore_index=True)
                else:
                    html_df = new_df

                item_list_len = resp_data["data"]["totalResultCount"]

                if "cacheId" in resp_data["data"]:
                    max_posted_Ts = resp_data["data"]["maxPostedTs"]
                    cache_id = resp_data["data"]["cacheId"]

                    for i in range(math.ceil(item_list_len/item_count_per_fetch)):
                        url_string = "https://sapi.craigslist.org/web/v8/postings/search/batch?batch="+area_id+"-"+str(i*item_count_per_fetch)+"-"+str(item_count_per_fetch)+ \
                                    "-1-0-"+str(max_posted_Ts)+"-"+str(current_secs)+"&cacheId="+cache_id+"&cc=US&lang=en"
                        #print(url_string)
                        response = requests.get(url_string)
                        response_code = response.status_code 

                        if (response_code >= 200 and response_code <= 299):
                            #Check each API call for failure
                            item_data = response.json()
                            item_returns.append(item_data)
                            pulled_extra_entries = True

                            api_item = {}
                            api_item["url"] = html.escape(url_string)
                            api_item["city"] = options["city_name"], 
                            api_item["area_id"] = area_id,
                            api_item["category"] = options["category_name"], 
                            api_item["type"] = "MULTI_API", 
                            api_item["order"] =  i,
                            api_item["response_text"] = json.dumps(item_data),
                            api_item["gathered_timestamp_utc"] = current_secs

                            new_df  = pd.DataFrame(api_item, columns = df_html_columns, index=[0])
                            if (len(html_df) > 0):
                                html_df = pd.concat([html_df,new_df],ignore_index=True)
                            else:
                                html_df = new_df

                        else:
                            failed = True
                else:
                        pulled_extra_entries = False
            else:
                failed = True
        else:
            failed = True

        if not failed:
            #We successfully got all our data, now parse it.
            item_id_base = resp_data["data"]["decode"]["minPostingId"]
            item_posted_base = resp_data["data"]["decode"]["minPostedDate"]
            gathered_timstamp = datetime.datetime.now(datetime.timezone.utc)
            fetched = False

            full_df = pd.DataFrame(columns=df_columns)
            for item_num in range(len(resp_data["data"]["items"])):
            
                try:
                    # Sometimes items will be left out.  Don't fail.
                    new_item = {}
                    new_item["gathered_timestamp_utc"] = gathered_timstamp
                    new_item["fetched"] = fetched
                    item = resp_data["data"]["items"][item_num]

                    new_item["item_id_int"] = item_id_base + item[0]
                    new_item["item_id"] = str(new_item["item_id_int"])
                    new_item["city_retrieve"] = options["city_name"]
                    new_item["category_retrieve"] = options["category_name"]
                    item_parse = item[4].split("~")
                    location_id = int(item_parse[0].split(":")[0])
                    location_description_id = int(item_parse[0].split(":")[1])
                    new_item["craigslist_location"] = resp_data["data"]["decode"]["locations"][location_id][1]
                    new_item["craigslist_location_description"] = resp_data["data"]["decode"]["locationDescriptions"][location_description_id]
                    if (len(item_parse) > 1):
                        new_item["craigslist_item_lat"] = item_parse[1]
                    else:
                        new_item["craigslist_item_lat"] = np.NaN
                    if (len(item_parse) > 1):
                        new_item["craigslist_item_long"] = item_parse[2]
                    else:
                        new_item["craigslist_item_long"] = np.NaN
                    new_item["posted_date_epoch_utc"] = item_posted_base + item[1]
                    new_item["posted_date_timestamp_utc"] =  datetime.datetime.fromtimestamp(new_item["posted_date_epoch_utc"],datetime.timezone.utc)
                    category_url = new_item["category_retrieve"]
                    if new_item["category_retrieve"] in category_to_url_converter.keys():
                        category_url = category_to_url_converter[new_item["category_retrieve"]]

                    new_item["item_price"] = ""
                    new_item["item_name"] = ""
                    new_item["item_url"] = ""
                    #Depending on wether we had >360 entries or <360 entries, we have to get the following items differently
                    if (pulled_extra_entries):
                        #If more than 360 entries, get data from individual batch pulls
                        item_data_list = item_returns[math.floor(item_num/item_count_per_fetch)]["data"]["batch"][item_num%item_count_per_fetch]
                    else:
                        #If less or equal to 360 entries, get data from full batch pull
                        item_data_list = item


                    for data_item in reversed(item_data_list): # Name is the last string element so reverse and pick the first
                        if type(data_item) == list:
                            if (data_item[0] == 10):
                                new_item["item_price"] = data_item[1]
                            elif (data_item[0] == 6):
                                new_item["item_url"] = "https://"+new_item["city_retrieve"]+".craigslist.org/"+category_url+"/d/"+ data_item[1]+"/"+new_item["item_id"]+".html"
                        elif type(data_item) == str and new_item["item_name"] == "":
                            new_item["item_name"] = data_item

                    print(new_item)
                    # Stack up the items - should make lists but rushing.
                    # shouldn't be too excessive unless > 50Kish? entries
                    new_df  = pd.DataFrame(new_item, columns = df_columns, index=[0])
                    if (len(full_df) > 0):
                        full_df = pd.concat([full_df,new_df],ignore_index=True)
                    else:
                        full_df = new_df
                except IndexError:
                    print(item)
                    print(item_data)

            print(len(full_df))
            #Write data to database:
            myDB = db.engine.url.URL.create(drivername="mysql",
                                    host="127.0.0.1",
                                    database = options["database"],
                                    query={"read_default_file" : options["mysqlconfig"], "charset" : "utf8mb4"},
                                    )

            engine = db.create_engine(url=myDB, pool_pre_ping=True)  

            if not options["table_append_timestamp"]:
                host_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_short"
            else:
                host_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_short" + "_" + str(math.floor(gathered_timstamp.to_seconds()))
            if options["replace_database_tables"]:
                db_if_exists = "replace"
            else:
                db_if_exists = "append"
            full_df.to_sql(name=host_table, con=engine, if_exists=db_if_exists, index=False)

            if not options["table_append_timestamp"]:
                host_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_html"
            else:
                host_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_html" + "_" + str(math.floor(gathered_timstamp.to_seconds()))

            # sqlalchemy_html_dtypes = {"url": db.types.String, 
            #                     "city": db.types.String, 
            #                     "category": db.types.String, 
            #                     "type": db.types.String, 
            #                     "order" : db.types.Integer,
            #                     "response_text":db.types.Text,
            #                     "gathered_timestamp_utc": db.types.DateTime}

            html_table = db.Table(host_table, db.MetaData(), 
                                  db.Column("url", db.String(500)),
                                  db.Column("city", db.String(100)),
                                  db.Column("area_id", db.Integer),
                                  db.Column("category", db.String(10)),
                                  db.Column("type", db.String(20)),
                                  db.Column("order", db.Integer),
                                  db.Column("response_text", db.Text(10000000)),
                                  db.Column("gathered_timestamp_utc", db.BigInteger),
                                  )
            
            if options["replace_database_tables"]:
                html_table.drop(engine, checkfirst=True)

            html_table.create(engine, checkfirst=True)
            html_df.to_sql(name=host_table, con=engine, if_exists="append", index=False)

            print("FETCH SUCCESS: CITY:"+options["city_name"]+":CATEGORY:"+options["category_name"])

        else:
            print("FETCH FAILURE: CITY:"+options["city_name"]+":CATEGORY:"+options["category_name"])

        print(datetime.datetime.now())
        time.sleep(30)
    # END LOOP
        
def usage():
    print("python " + str(sys.argv[0]) + "[--city [craigslist city name]] [--category [3 letter craigslist category id or \"all\"]] [--database [database tables go in]] \n" +
                                         " [--replace_tables] [--table_append_timestamp] \n" +
                                         " \'replace_tables\' and \'table_append_timestamp\' default to FALSE.  Use the flags to set to TRUE") 

if __name__ == "__main__":
    main()