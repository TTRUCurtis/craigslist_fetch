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
import time
import random


def main():
    options = {}
    options["debug_level"] = 1
    options["mysqlconfig"] = str(str(Path.home()) + '/.my.cnf')
    options["database"] = "douglasvbellew"
    options["city_name"] = "losangeles"
    options["category_name"] = "ava"
    options["replace_database_tables"] = True
    options["table_append_timestamp"] = False
    options["use_timestamp"] = ""

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
                "item_long_description":pd.Series(dtype='str'),
                "posted_date_epoch_utc":pd.Series(dtype='int'),
                "posted_date_timestamp_utc":pd.Series(dtype="datetime64[ns]"),
                "gathered_timestamp_utc":pd.Series(dtype="datetime64[ns]"), 
                "fetched":pd.Series(dtype='bool')}
    
    try:
        optlist, args = getopt.getopt(sys.argv[1:], "", ["city=", "category=", "database=", "use_timestamp=","replace_database_tables"])
    except getopt.GetoptError as err:
        print(err)
        usage()
        sys.exit(2)

    for option_tuple in optlist:
        if (option_tuple[0] == "--city"):
            options["city_name"] = option_tuple[1]
        elif (option_tuple[0] == "--category"):
            print("Override category to:" + option_tuple[1])
            options["category_name"] = option_tuple[1]
        elif (option_tuple[0] == "--database"):
            if (option_tuple[1] == ""):
                usage()
                sys.exit(2)
            else:
                options["database"] = option_tuple[1]
        elif (option_tuple[0] == "--use_timestamp"):
            options["use_timestamp"] = option_tuple[1]
        elif (option_tuple[0] == "--replace_database_tables"):
            print("Set replace_database_tables to True")
            options["replace_database_tables"] = True

    # START LOOP HERE (LOOP OVER CITY/CATEGORY) make sure to put in some form of sleep between each get so you don't
    # get tagged by the server.  (20 secs per category?) 

    item_categories = [options["category_name"]]

    if (item_categories[0] == "all") :
        item_categories = name_to_category_converter.values()

    for category in item_categories:

        print("FETCHING CITY:"+options["city_name"]+":CATEGORY:"+options["category_name"])
        print(datetime.datetime.now())
        options["category_name"] = category


        myDB = db.engine.url.URL.create(drivername="mysql",
                                host="127.0.0.1",
                                database = options["database"],
                                query={"read_default_file" : options["mysqlconfig"], "charset" : "utf8mb4"},
                                )

        engine = db.create_engine(url=myDB, pool_pre_ping=True)   

        #See if we have any previously fetched items.  Don't want to re-fetch unless item post timestamp is greater than the fetch time
        #If we're just overwriting the table, don't bother.
        if not options["replace_database_tables"]:
            if options["use_timestamp"] != "":
                source_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_long" +"_"+options["use_timestamp"]
            else:
                source_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_long"
            with engine.connect() as connection:
                try:
                    print(source_table)
                    metadata  = db.MetaData()
                    orig_data = db.Table(source_table, metadata, autoload_with=engine)         
                    query = db.select(orig_data)
                    result_proxy = connection.execute(query)
                    result_set = result_proxy.fetchall()

                    if (len(result_set) > 0):
                    # Put Result into PANDAS dataframe
                        existing_item_df = pd.DataFrame(result_set)
                        existing_item_df.columns = orig_data.columns.keys()
                        existing_item_df["fetched"] = existing_item_df["fetched"].astype("bool")
                        if (options["debug_level"] != 0):
                            print("Retrieved "+ str(len(existing_item_df))+ " rows.")
                    else:
                        existing_item_df = pd.DataFrame(columns=df_columns)
                except Exception as e:
                    print(e)
                    existing_item_df = pd.DataFrame(columns=df_columns)
        else:
            existing_item_df = pd.DataFrame(columns=df_columns)

        #pull data from the list table to get our list of ids to pull. 
        skip_category = False
        with engine.connect() as connection:        
            if options["use_timestamp"] != "":
                source_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_short" +"_"+options["use_timestamp"]
            else:
                source_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_short"
            with engine.connect() as connection:
                try:
                    print(source_table)
                    metadata  = db.MetaData()
                    orig_data = db.Table(source_table, metadata, autoload_with=engine)         
                    query = db.select(orig_data)
                    result_proxy = connection.execute(query)
                    result_set = result_proxy.fetchall()

                    if (len(result_set) > 0):
                    # Put Result into PANDAS dataframe
                        pd.set_option("max_colwidth",30)
                        pd.set_option("large_repr", "truncate")
                        pd.set_option("display.width", None)
                        item_list_df = pd.DataFrame(result_set)
                        item_list_df.columns = orig_data.columns.keys()
                        item_list_df["fetched"] = item_list_df["fetched"].astype("bool")
                        if (options["debug_level"] != 0):
                            print("Retrieved "+ str(len(item_list_df))+ " rows.")
                    else:
                        print("No items in table: "+source_table+". Skipping category.")
                        skip_category = True
                        #sys.exit(2)
                except Exception as e:
                    print(e)
                    print("Failed accessing table: "+source_table+". Skipping category.")
                    skip_category = True
                    #sys.exit(2) 

        if skip_category:
            continue # No category data... move to the next category
        
        #Only get the latest version of each item in each table to compare
        item_list_groups_df = item_list_df.sort_values(["item_id","posted_date_epoch_utc"], ascending=False).groupby("item_id").head(1)
        existing_item_groups_df = existing_item_df.sort_values(["item_id","posted_date_epoch_utc"], ascending=False).groupby("item_id").head(1)

    #    print(existing_item_groups_df)
        if len(existing_item_groups_df) > 0:
            df_all = item_list_groups_df.merge(existing_item_groups_df[["item_id","posted_date_epoch_utc"]], on=["item_id"], how = "outer", indicator=True)

            # Get list of items to fetch. 
            # Only fetch items if new or posted date is greater than fetched posted date (updated)
            for row_num in range(len(df_all)):
                if (df_all.loc[row_num,"_merge"] == "both"):
                    if df_all.loc[row_num,"posted_date_epoch_utc_x"] <= df_all.loc[row_num,"posted_date_epoch_utc_y"] :
                        df_all.loc[row_num,"fetched"] = True
                elif df_all.loc[row_num,"_merge"] == "right_only":
                    df_all.loc[row_num,"fetched"] = True
            df_fetch = df_all[df_all["fetched"] == False].copy(deep=True)

            df_fetch.drop(columns=["posted_date_epoch_utc_y","_merge"],inplace=True)
            df_fetch.rename(columns={"posted_date_epoch_utc_x":"posted_date_epoch_utc"}, inplace=True)
        else:
            df_fetch = item_list_groups_df 

        #Check if we have valid fetch items
        fetched_rows = 0
        if len(df_fetch) > 0:
            # Have valid Fetch items.
            if (options["debug_level"] != 0):
                print("Sending "+ str(len(df_fetch))+" rows." )
    
            df_fetch.loc[:,"item_long_description"] = ""

            if not options["table_append_timestamp"]:
                host_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_long"
            else:
                host_table = "craigslist_"+options["city_name"]+"_"+options["category_name"] + "_long" + "_" + options["use_timestamp"]
            if options["replace_database_tables"]:
                db_if_exists = "replace"
            else:
                db_if_exists = "append"

            for row_num in range(len(df_fetch)):
                url_string = df_fetch.loc[row_num,"item_url"]

                try:
                    if (options["debug_level"] != 0):
                        if (row_num %10 == 0):
                            print("Starting row "+ str(row_num))
                            #print(url_string)                    

                    response = requests.get(url_string)
                    response_code = response.status_code

                    if (response_code >= 200 and response_code <= 299):
                        soup = BeautifulSoup(response.content, "html.parser")
                        data_tag = soup.find_all(name="section", id="postingbody",limit=1)[0]
                        #print(data_tag, flush=True)
                        data_tag.p.extract()
                        data_tag.div.extract()
                        df_fetch.loc[row_num,"item_long_description"] = data_tag.get_text()
                        df_fetch.loc[row_num,"page_html"] = response.content
                        #print(df_fetch.loc[row_num,"item_long_description"], flush=True)
                        df_send = df_fetch.loc[[row_num],:]

                        data_tag_arr = soup.find_all(name="span", class_="otherpostings", limit=1)
                        print(data_tag_arr)
                        if (len(data_tag_arr) == 0):
                            print("No more ads tag")
                            df_fetch.loc[row_num,"more_ads_html"] = ""
                            df_fetch.loc[row_num,"more_ads_count"] = 0
                        else:
                            more_ads_url = data_tag_arr[0].a.attr("href")
                            print(more_ads_url)

                        #print(df_send)
                        # Note: Normally we'd batch these up and send them altogether, however, since we're waiting 3-5 seconds
                        # between each one... who cares and it gives us recovery from exactly where we left off if we crash.
                        # Only drop the table on the initial write if we want to replace and not append
                        if (fetched_rows == 0):   
                        
                            long_table = db.Table(host_table, db.MetaData(), 
                                                db.Column("item_id", db.Text(500)),
                                                db.Column("item_id_int", db.BigInteger),
                                                db.Column("item_url", db.Text(500)),
                                                db.Column("city_retrieve", db.Text(500)),
                                                db.Column("category_retrieve",db.Text(500)),
                                                db.Column("craigslist_location", db.Text(500)),
                                                db.Column("craigslist_location_description", db.Text(500)),
                                                db.Column("craigslist_item_lat", db.String(500)),
                                                db.Column("craigslist_item_long", db.String(500)),
                                                db.Column("item_name", db.Text(500)),
                                                db.Column("item_price", db.Text(500)),
                                                db.Column("posted_date_epoch_utc", db.BigInteger),
                                                db.Column("posted_date_timestamp_utc", db.DateTime),
                                                db.Column("gathered_timestamp_utc", db.DateTime),
                                                db.Column("fetched", db.Boolean),
                                                db.Column("item_long_description", db.Text(500)),
                                                db.Column("page_html", db.Text(10000000)),
                                                db.Column("more_ads_html", db.Text(10000000)),
                                                db.Column("more_ads_count", db.Integer),
                                                )
                            
                            if options["replace_database_tables"]:
                                long_table.drop(engine, checkfirst=True)

                            long_table.create(engine, checkfirst=True) 

                            df_send.to_sql(name=host_table, con=engine, if_exists=db_if_exists, index=False)
                        else:
                            df_send.to_sql(name=host_table, con=engine, if_exists="append", index=False)
                        fetched_rows = fetched_rows + 1

                except Exception as e:
                        #something happened (unavailable url or data value).  skip row
                        print(e)

                #Put random sleep in so don't get flagged as scraper.
                time.sleep(3 + random.random() * 2)

        if fetched_rows > 0:
            print("FETCH SUCCESS: SENT:"+str(fetched_rows)+" rows to table:"+host_table)
        else:
            print("NO VALID ROWS TO SEND TO DETAIL TABLE.")

        print(datetime.datetime.now())
        time.sleep(30)
    # END LOOP

def usage():
    print("python " + str(sys.argv[0]) + "[--city [craigslist city name]] [--category [3 letter craigslist category id or \"all\"]] [--database [database tables go in]] \n" +
                                         " [--use_timestamp [appended timestamp of table to use]] [--replace_database_tables] \n" +
                                         " \'use_timestamp\' defaults to \"\".  Leave it unset to use the base table. \n" +
                                         " \'replace_database_tables\' defaults to FALSE.  Use the flag to set to TRUE to overwrite the result table which requests new data for every id.") 

if __name__ == "__main__":
    main()