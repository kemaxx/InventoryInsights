import gspread as gc
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import time
from datetime import datetime,timedelta
import numpy as np
import random as rd
from prophet import Prophet
import os
from dotenv import load_dotenv

load_dotenv()



class PriceTrackingAutomator:
    
    """
    This project automates the tracking of prices at a significant differential change - say 10%
    """

    def __init__(self):
        self.gc = gc.service_account("steam-talent-404201-0536079b7ca9.json")
        self.voucher_df = self.get_voucher_df()

    def get_all_stock_db(self):
        all_stock_sheet = self.gc.open_by_key("1qqI-9I99Kix2PS1ksUralHeFXoyaArN7ZXYmCnMDLA0")
        all_stock_workbook = all_stock_sheet.worksheet(os.getenv("STOCK_DB_API_KEY"))
        all_stock_record = all_stock_workbook.get_all_records()

        cleaned_records_list = list()

        for record in all_stock_record:
            cleaned_record_dict = dict()
            for key, item in record.items():

                key = key.replace('"', '')
                if isinstance(item, str):
                    item = item.replace('"', '')
                else:
                    item = item
                cleaned_record_dict[key] = item

            cleaned_records_list.append(cleaned_record_dict)

        df = pd.DataFrame(cleaned_records_list)
        df["Rate"] = df["Rate"].astype(float)

        # Process for vital categories.
        cats = ["DRINKS", "WINE", "FOOD ITEM", "BITE", "BEVERAGE"]
        df = df.loc[df.Category.isin(cats), :]
        return df
    
    def get_voucher_df(self):
        """"
        This method fetches and returns issued stocks as a Pandas Dataframe
        """
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("ISSUED_STOCK_API_KEY"))
        worksheet = sheet.worksheet("Issues")
        record_list = worksheet.get_all_values()
       
        # Clean Data
        record_list = [[str(cell).replace('"', '') for cell in row] for row in record_list]
        df = pd.DataFrame(data=record_list[1:], columns=record_list[0])
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")
        df["Usage"] = df["Usage"].astype(float)
        
        return df


    def get_most_relevant_stock(self, n_items=150):

        """This function gets 150 most used items in the Hotel"""
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("ISSUED_STOCK_API_KEY"))
        worksheet = sheet.worksheet("Issues")
        record_list = worksheet.get_all_values()

        exclude_these_items = ['ORIGIN BITTERS SMALL', 'ALBARKA TABLE WATER (50CL)', 'FAYROUZ CAN',
                               'PINEAPPLE JUICE DRINK',
                               'ACE ROOT', 'ZAGG (CAN)', 'ZAGG CAN', 'VEGETABLE (STAFF)',
                               'TOMATO FLAVOR SEASONING (CUBE)']

        add_these = ["CAT FISH", "CAT FISH (SMALL)","SWAN WATER","GOLDBERG BLACK (45cl)","LEGEND TWIST","4TH STREET (BIG)"]

        # Clean Data
        record_list = [[str(cell).replace('"', '') for cell in row] for row in record_list]
        df = pd.DataFrame(data=record_list[1:], columns=record_list[0])
        df["Date"] = pd.to_datetime(df["Date"], format="%Y-%m-%d")

        df = df.mask(df["Item name"].isin(exclude_these_items), np.nan).dropna()

        cats = ["DRINKS", "WINE", "FOOD ITEM", "BITE", "BEVERAGE", "CLEANING SUPPLY", "GUEST SUPPLY"]
        df2 = df.loc[df["Category"].isin(cats), :]
        relevant_items = df2["Item name"].value_counts()[:n_items].index.tolist()

        cats2 = ["DRINKS", "WINE"]
        excluded_items = df2["Item name"].value_counts()[n_items:].index.tolist()
        ex_df = df2.loc[(df2['Category'].isin(cats2)) & (df2["Item name"].isin(excluded_items)), :]
        excluded_list = ex_df["Item name"].unique().tolist()

        all_relevant_items = sorted(list(set(relevant_items + excluded_list + add_these)))

        return all_relevant_items

    def get_stock_portions(self, importance_stock=[]):
        """
        This method takes a list of n-most relevant items and gets the units of those stocks
        from the all-stock Google Sheet database
        """
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("STOCK_DB_API_KEY"))
        workbook = sheet.worksheet("My Stock")
        data = workbook.get_all_values()

        data = [[str(cell).replace('"', '') for cell in row] for row in data]
        df = pd.DataFrame(data=data[1:], columns=data[0])

        # sift for relevance
        df = df.loc[df['Stock Name'].isin(importance_stock), :]
        units = df["Ptn Name"].tolist()
        return units

    def process_for_base_previous_current_costs(self, relevant_items=[]):
        """
        This method fetches and processes three Google Sheets:
        a)The Base Cost Db.
        b) The Previous Cost Db.
        c) The urrent Cost Db.

        This method should return a two pandas Dataframe: the first is for email notifications
        and the second is for Google sheet dashboard update.

        """
        # a) Getting the Base Cost Db:

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("BASE_COST_PRICE_API_KEY"))
        worksheet = sheet.worksheet("Base Cost")
        base_cost_record_list = worksheet.get_all_values()

        base_cost_df = pd.DataFrame(data=base_cost_record_list[1:], columns=base_cost_record_list[0])
        base_cost_df["Cost price"] = base_cost_df["Cost price"].str.replace(",", "").astype(float)
        base_cost_df = base_cost_df[["Stock name", "Cost price"]]

        time.sleep(1)
        # b) Getting the Previous Cost Db:

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("PREV_COST_PRICE_API_KEY"))
        worksheet = sheet.worksheet("Previous Costs")
        previous_record_list = worksheet.get_all_values()

        previous_cost_df = pd.DataFrame(data=previous_record_list[1:], columns=previous_record_list[0])
        previous_cost_df["Cost price"] = previous_cost_df["Cost price"].str.replace(",", "").astype(float)
        previous_cost_df = previous_cost_df[["Stock name", "Cost price"]]

        time.sleep(1)
        # c) Getting the Current Cost Db:

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("CURRENT_COST_PRICE_API_KEY"))
        worksheet = sheet.worksheet("Current Costs")
        current_cost_record_list = worksheet.get_all_values()

        current_cost_df = pd.DataFrame(data=current_cost_record_list[1:], columns=current_cost_record_list[0])
        current_cost_df["Cost price"] = current_cost_df["Cost price"].str.replace(",", "").astype(float)
        current_cost_df = current_cost_df[["Stock name", "Cost price"]]

        # Merge these dfs based on stock names
        merged_dfs1 = pd.merge(base_cost_df, previous_cost_df, on="Stock name")
        merged_all = merged_dfs1.merge(current_cost_df, on="Stock name")
        filtered_for_rerlevance_df = merged_all.loc[merged_all["Stock name"].isin(relevant_items)]
        filtered_for_rerlevance_df_items_list = filtered_for_rerlevance_df["Stock name"].unique().tolist()
        items_units = self.get_stock_portions(filtered_for_rerlevance_df_items_list)
        filtered_for_rerlevance_df["Unit Name"] = items_units
        # Cost price_x	Cost price_y	Cost price	Unit Name
        filtered_for_rerlevance_df = filtered_for_rerlevance_df[
            ["Stock name", "Unit Name", "Cost price_x", "Cost price_y", "Cost price"]]
        columns = ["Stock Name", "Unit Name", "Base Cost Price (₦)", "Prev Cost Price (₦)", "Current Cost Price (₦)"]
        filtered_for_rerlevance_df.columns = columns

        # 4. Compute for price changes
        filtered_for_rerlevance_df["Percentage_Change"] = round((filtered_for_rerlevance_df["Current Cost Price (₦)"] - \
                                                                 filtered_for_rerlevance_df[
                                                                     "Base Cost Price (₦)"]) * 100 /
                                                                filtered_for_rerlevance_df["Current Cost Price (₦)"], 2)

        # sheet_df.loc[~(sheet_df["Percentage_Change"]==0) & (sheet_df["Percentage_Change"]>=10),:]

        # Computing for Significance
        filtered_for_rerlevance_df["Is_Significant"] = (filtered_for_rerlevance_df["Percentage_Change"] > 10) | (
                filtered_for_rerlevance_df["Percentage_Change"] < -10)

        ## Convert Base Cost to Currrent Cost if Change is Significant!

        filtered_for_rerlevance_df_stock_names = filtered_for_rerlevance_df["Stock Name"].tolist()
        significance = filtered_for_rerlevance_df["Is_Significant"].tolist()
        current_cost_list = filtered_for_rerlevance_df["Current Cost Price (₦)"].tolist()

        filtered_for_rerlevance_df_for_email = filtered_for_rerlevance_df.copy()

        # This part prepares the df for Google sheet Deploymanet
        filtered_for_rerlevance_df_for_google_dashboard = filtered_for_rerlevance_df.set_index("Stock Name")

        for stock, significant, current_cost in zip(filtered_for_rerlevance_df_stock_names, significance,
                                                    current_cost_list):
            if bool(significant):
                filtered_for_rerlevance_df_for_google_dashboard.loc[stock, "Base Cost Price (₦)"] = current_cost

        return filtered_for_rerlevance_df_for_email, filtered_for_rerlevance_df_for_google_dashboard.reset_index()

    def get_outliers_from_df_rates(self,df):
        """
        This method filters out the outliers from a rates list
        :return: It returns a list of the outliers in a distribution
        """

        item_rates = df["Rate"].tolist()
        Q1 = np.percentile(item_rates, 25)
        Q3 = np.percentile(item_rates, 75)

        IQR = Q3 - Q1

        # Define threshold for outliers
        threshold = 1.5 * IQR

        outliers = [x for x in item_rates if x < Q1 - threshold or x > Q3 + threshold]

        return outliers

    def get_all_purchases(self):
        """
        This method fetches all purchases from Google Sheets
        :return: It returns a dataframe of the fetched purhases.
        """
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("PURCHASE_DB_API_KEY"))
        workbook = sheet.worksheet("Purchases")

        values = workbook.get_all_values()

        df = pd.DataFrame(data = values[1:],columns=values[0])
        df["Date"] = pd.to_datetime(df["Date"],format="%Y-%m-%d")
        df["Rate"] = df["Rate"].astype(float)
        df["Qty"] = df["Qty"].astype(float)
        df["Amount"] = df["Amount"].astype(float)

        return df

    def get_stock_category(self):
        """
        This method fetchest the categories of stocks
        :return: it returns a dictionary for stock category
        """
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("STOCK_DB_API_KEY"))
        workbook = sheet.worksheet("My Stock")

        data = workbook.get_all_values()
        formated_data = [[str(cell).replace('"', '') for cell in row] for row in data]

        df = pd.DataFrame(formated_data[1:], columns=formated_data[0])
        df["Bundle Qty"] = df["Bundle Qty"].astype(float)

        stock_category_dict = dict(zip(df["Stock Name"], df["Category"]))

        return stock_category_dict

    def get_insights_for_3wk_ma(self,ITEM_NAME):

        """
        This method provides a random text template of an insight for it use
        :return: Returns a random insight.
        """
        INSIGHTS = [
            f"The current price of {ITEM_NAME} is higher than the 3-week average, suggesting an upward trend. To mitigate future price increases and ensure customer supply, we should consider stocking up now.",
            f"""
            {ITEM_NAME} is currently more expensive than its rolling three-week average, which could indicate a price hike. In order to lessen this, inventory needs should be evaluated while taking demand, storage capacity, and budgetary restrictions into account.
            """,
            f"""
            The price of {ITEM_NAME} is currently higher than its three-week moving average from the preceding three weeks, suggesting a possible price increase. It would be prudent to assess inventory requirements at this time, keeping in mind demand, storage capacity, and financial constraints. Now would be a good time to stock up.
            """,
            f"""
            The current price of {ITEM_NAME} is higher than it's previous rolling average, indicating a potential price increase. To protect against future price increases, we should evaluate inventory needs and consider restocking, considering factors like demand, storage capacity, and budget.
            """

        ]
        return rd.choice(INSIGHTS)

    def compute_moving_average_for_sig_items(self,sig_change_items=[("GULDER",34,'food item'),("TROPHY",34,'food item')]):
        """
        This method computes the moving average of Wines and Drinks
        :return: It returns a dictionary containing the most recent 3-week moving averages for each stocks
        """
        purchase_df = self.get_all_purchases()
        three_wk_rolling_mean_dict = dict()

        for stock in sig_change_items:
            item_df = purchase_df.loc[purchase_df["Stock name"]==stock[0],:]

            outliers_list = self.get_outliers_from_df_rates(item_df)

            item_df = item_df.loc[~(item_df["Rate"].isin(outliers_list)),:]

            # Compute 3_wk moving average

            item_df["3_wk_ma"] = item_df["Rate"].rolling(window=21).mean()
            three_wk_rolling_mean_dict[stock[0]] = round(item_df["3_wk_ma"].tolist()[-1],2)

        return three_wk_rolling_mean_dict

    def modify_sig_change_list_decorator(func):
        """
        The aim of this method is to return a modified list of tuples which now includes some insights
        regarding a stock if it's a 'Wine' or 'Drinks'
        :param func:
        :return:
        """
        def wrapper_function(self,*args,**kwargs):

            sig_item_list = func(self,*args,**kwargs)
            stock_cats_dict = self.get_stock_category()
            purchase_df = self.get_all_purchases()

            for record in sig_item_list:
                if stock_cats_dict[record["Stock Name"]] in ["DRINKS","WINE"]:
                    item_df = purchase_df.loc[purchase_df["Stock name"]==record["Stock Name"],:]

                    outliers_list = self.get_outliers_from_df_rates(item_df)

                    item_df = item_df.loc[~(item_df["Rate"].isin(outliers_list)),:]

                    item_df["3_wk_ma"] = item_df["Rate"].rolling(window=21).mean()
                    recent_rolling_mean = item_df["3_wk_ma"].tolist()[-1]
                    print(record["Stock Name"] ,"=",str(recent_rolling_mean))

                    current_cost = float(record['Current Cost Price'].replace(",",''))

                    if current_cost>recent_rolling_mean:
                        ratio = (current_cost / recent_rolling_mean)
                        deviation = ratio-1
                        print("ratio "+str(ratio))
                        significance_threshold = 0.1  # Deviation cut off of 10%
                        if deviation >= significance_threshold:
                            record["Insights"] = self.get_insights_for_3wk_ma(record["Stock Name"]).strip()
            return sig_item_list

        return wrapper_function
    
    def get_weekly_forecast_df(self,stock_name="PEAK SATCHET (14g)"):

      hnk = self.voucher_df.loc[self.voucher_df['Item name']==stock_name,:]
      hnk["Bf_Qty"] = hnk["Usage"].astype(float)
      hnk = hnk.set_index("Date")
      hnk = hnk.resample("W").sum()
      hnk.reset_index(inplace=True)
      data = hnk[['Date','Bf_Qty']].reset_index()
      data = data[["Date","Bf_Qty"]].rename({"Date":"ds","Bf_Qty":"y"},axis='columns')
      
      model = Prophet()
      model.fit(data)
      future = model.make_future_dataframe(periods=1,freq='W')
      forecast = model.predict(future)
      return forecast.tail(3).round(0)
    
    def actual_demand(self,stock_name="PEAK SATCHET (14g)",date_range=()):
      # Checking forcast - forcast said we'll need about 598 with a range of (215.702342 TO 959.406705) for week 2024-07-28 on SWAN WATER. How true now?
      # Lets sum up all Heinken consumption from 2024-07-22 up until 2024-07-28

      weekly_data = self.voucher_df.loc[(self.voucher_df["Date"].between(*date_range)) &
                                       (self.voucher_df["Item name"]==stock_name),:]
      
      if not weekly_data.empty:
          
        return weekly_data["Usage"].sum()
      
      else:
          return 0.0
     

    def modify_sig_list_with_weekly_forecast_data_decorator(func):
      def wrapper_function(self,*args,**kwargs):
          sig_list = func(self,*args,**kwargs)
          stock_cats_dict = self.get_stock_category()
          category_to_decorate = ["WINE","DRINKS"]

          for record in sig_list:
              category = stock_cats_dict[record["Stock Name"]]
              if category in category_to_decorate:
                  prophet_df = self.get_weekly_forecast_df(stock_name=record["Stock Name"])

                  start_date = str(prophet_df.iloc[0]['ds'] + timedelta(days=1)).split("T")[0]
                  end_date = str(prophet_df.iloc[1]['ds']).split("T")[0]

                 # get commentary

                #Previous week mean demand forecast
                  prev_week_forecast_df = prophet_df.head()
                  prev_forecast = prev_week_forecast_df.iloc[1]['yhat']
                


                  # Previous actual demand
                  actual_demand_ = self.actual_demand(stock_name=record["Stock Name"],date_range=(start_date,end_date))

                  #Forecast Accuracy
                  percentage = round((actual_demand_/prev_forecast)*100,2)

                  # Upcoming week forecast
                  upcoming_week_df = prophet_df.tail()
                  upcoming_forecast = float(upcoming_week_df.iloc[2]['yhat'])
                  upcoming_week_lower_ci = 0 if float(upcoming_week_df.iloc[2]['yhat_lower'])<0 else float(upcoming_week_df.iloc[2]['yhat_lower'])
                  upcoming_week_upper_ci = 0 if float(upcoming_week_df.iloc[2]['yhat_upper'])<0 else float(upcoming_week_df.iloc[2]['yhat_upper']) 
                  
                  upcoming_week_ci = upcoming_week_lower_ci,upcoming_week_upper_ci


                  record["Current Week Forecast"] = prev_forecast
                  record["Actual Units Issued"] = actual_demand_
                  record["Forecast Accuracy (%)"] = percentage
                  record["Upcoming Week Forecast"] = upcoming_forecast
                  record["Upcoming Week 95% CI Forecast"] = upcoming_week_ci

                  #self.actual_demand(stock_name=record["Stock Name"],date_range=(start_date,end_date))
          return sig_list
      return wrapper_function
              
              





    #@modify_sig_change_list_decorator
    @modify_sig_list_with_weekly_forecast_data_decorator
    def check_inventory_db_for_price_change_significance(self, df):
        """This method compute for significance of price changes and returns
        a list of tuples of stocks with their details"""

        # 5. Creatinh a list of tuples for items whose percentage are significant.
        sheet_df_dict_list = df.to_dict(orient='records')
        items_with_sig_change = list()
        for record in sheet_df_dict_list:

            # <span style = "color: red;"> This text is red. < / p >
            tup = ()
            item_dict = dict()
            if record["Is_Significant"]:
                if record["Percentage_Change"]>=10:
                    item_dict["Stock Name"] = record["Stock Name"]
                    item_dict["Base Cost Price"] = "{:,}".format(record['Base Cost Price (₦)'])
                    item_dict['Prev Cost Price'] = "{:,}".format(record['Prev Cost Price (₦)'])
                    item_dict['Current Cost Price'] = "{:,}".format(record['Current Cost Price (₦)'])
                    item_dict['Percentage_Change'] = "+ "+str(record['Percentage_Change'])+"% 📈"
                    #sig_item = tup + (item_dict,)
                    items_with_sig_change.append(item_dict)

                    # sig_item = tup + (
                    # record["Stock Name"], "{:,}".format(record['Base Cost Price (₦)']), "{:,}".format(record['Prev Cost Price (₦)']),
                    # "{:,}".format(record['Current Cost Price (₦)']), "+ "+str(record['Percentage_Change'])+"% 📈")
                    # items_with_sig_change.append(sig_item)
                else:

                    item_dict["Stock Name"] = record["Stock Name"]
                    item_dict["Base Cost Price"] = "{:,}".format(record['Base Cost Price (₦)'])
                    item_dict['Prev Cost Price'] = "{:,}".format(record['Prev Cost Price (₦)'])
                    item_dict['Current Cost Price'] = "{:,}".format(record['Current Cost Price (₦)'])
                    item_dict['Percentage_Change'] = str(record['Percentage_Change']) + "% 📉"
                    #sig_item = tup + (item_dict,)
                    items_with_sig_change.append(item_dict)

                    # sig_item = tup + (
                    #     record["Stock Name"], "{:,}".format(record['Base Cost Price (₦)']), "{:,}".format(record['Prev Cost Price (₦)']),
                    #     "{:,}".format(record['Current Cost Price (₦)']), str(record['Percentage_Change']) + "% 📉")
                    # items_with_sig_change.append(sig_item)

        return items_with_sig_change

    def update_base_cost_db_with_new_base_costs(self, significant_items=()):

        """This method updates the base costs of the stocks whose price changes are significant"""

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("BASE_COST_PRICE_API_KEY"))
        worksheet = sheet.worksheet("Base Cost")

        record_list = worksheet.get_all_values()
        sheet_df = pd.DataFrame(record_list[1:], columns=record_list[0])
        sheet_df['Cost price'] = sheet_df['Cost price'].str.replace(",", "").astype(float)

        sheet_df.set_index("Stock name", inplace=True)

        for record in significant_items:
            sheet_df.loc[record.get("Stock Name"), "Cost price"] = record.get('Current Cost Price')

        sheet_df.reset_index(inplace=True)

        columns = sheet_df.columns.tolist()
        data = sheet_df.values.tolist()
        new_sheet_data = [[str(value) for value in record] for record in data]

        worksheet.clear()
        worksheet.append_rows([columns])
        worksheet.append_rows(new_sheet_data)

        print("Base Costs Database Has Been Successfully Updated!")
        return

    def update_previous_cost_db_with_current_cost(self):
        """This method updates the previous cost database
        Note: This should be done only after the Google Sheets Dashboard has been updated and the email sent to
        stakeholders!
        """
        ###********* PLEASE, ENSURE THIS PART IS DOING WHAT NEEDS TO BE DONE GOING FORWARD!!!****####
        time.sleep(1)
        # b) Getting the Previous Cost Db:
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("PREV_COST_PRICE_API_KEY"))
        prev_worksheet = sheet.worksheet("Previous Costs")

        time.sleep(1)
        # c) Getting the Current Cost Db:
        curr_account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        curr_sheet = curr_account.open_by_key(os.getenv("CURRENT_COST_PRICE_API_KEY"))
        current_worksheet = curr_sheet.worksheet("Current Costs") # RUN THIS NOW!!!
        current_cost_record_list = current_worksheet.get_all_values()

        prev_worksheet.clear()
        prev_worksheet.append_rows(current_cost_record_list)
        print("Previous Cost Database Has Been Successfully Updated!")
        return

    def create_a_column_for_each_sig_price_change(self, list_of_items_with_sig_change=()):
        date = datetime.now().date().strftime("%Y-%m-%d")

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("WEEKLY_DB_UPDATE_API_KEY"))
        weeklywork_sheet = sheet.worksheet("Weekly Changes")
        weekly_work_sheet_list = weeklywork_sheet.get_all_values()

        df = pd.DataFrame(data=weekly_work_sheet_list[1:], columns=weekly_work_sheet_list[0])
        df = df.set_index("Stock Name")

        for item in list_of_items_with_sig_change:
            df.loc[item.get("Stock Name"), f"Date_{date}"] = item.get('Current Cost Price')

        df = df.reset_index()
        columns = df.columns.tolist()
        data = df.values.tolist()
        sheet_compliant_data = [[str(cell) for cell in record] for record in data]

        weeklywork_sheet.clear()
        weeklywork_sheet.append_rows([columns])
        weeklywork_sheet.append_rows(sheet_compliant_data)

        print("Weekly Price Change Updated!")
        return

    def update_google_sheet_dashboard(self, df):
        """
        :param df: This refers to the processed df from the dashboard
        :return: It returns a message success if the db was successful.
        """
        columns = ['Stock Name', 'Unit Name', 'Base Cost Price (₦)', 'Prev Cost Price (₦)', 'Current Cost Price (₦)']
        range_cells_for_deletion = ["A5:E2000"]
        df = df[columns]
        data = df.values.tolist()

        # format the data for Google Sheets
        formated_data = [[str(cell) for cell in record] for record in data]

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("DASHBOARD_API_KEY"))
        worksheet = sheet.worksheet("Ken's Store")

        worksheet.batch_clear(range_cells_for_deletion)
        worksheet.append_rows(formated_data)
        print("Dashboard was Updated Successfully!")
        return

    def sent_email(self, list_of_changed_stock_prices=[]):
        print(list_of_changed_stock_prices)


        password = "mozw xyhc caqy kspq"
        username = "kenworkschool@gmail.com"
        receiver_mail_email_list = ["kennethdmark@gmail.com","ezevictor84@yahoo.com","ojmailing@gmail.com","austineze100@gmail.com"]
        redundant_list = ["evelynbrowny767@gmail.com",]


        if list_of_changed_stock_prices:
            details = list()
            for stock in list_of_changed_stock_prices:
                if "Current Week Forecast" in stock:
                    

                    info = f"""
                    <html>
                      <head>
                        <style>
                          body {{
                            font-family: Arial, sans-serif;
                            font-size: 16px;
                            line-height: 1.6;
                            color: #333;
                            margin: 0;
                            padding: 20px;
                          }}
                          h1, h2, h3, h4, h5, h6 {{
                            font-weight: bold;
                          }}
                          p {{
                            margin: 0 0 20px;
                          }}
                          strong {{
                            font-weight: bold;
                          }}
                          i.new {{
                            font-style: italic;
                            color: green; /* Green color for "New!" label */
                          }}
                          hr {{
                            border: none;
                            border-top: 1px solid #ccc;
                            margin: 20px 0;
                          }}
                          table {{
                            width: 100%;
                            border-collapse: collapse;
                          }}
                          th, td {{
                              border: 1px solid black;
                              padding: 8px;
                              text-align: center;
                          }}
                          th {{
                              background-color: #f2f2f2;
                          }}
                        </style>
                      </head>
                      <body>
                        <h1>{stock.get('Stock Name')}</h1>
                        <p>Base Cost Price: ₦{stock.get('Base Cost Price')}</p>
                        <p>Previous Cost Price: ₦{stock.get('Prev Cost Price')}</p>
                        <p>Current Cost Price: ₦{stock.get('Current Cost Price')}</p>
                        <p>Percentage Change: <strong>{stock.get('Percentage_Change')}</strong></p>
                        
                        <!--<p>Previous Week Mean Forecast <i class="new">(New!)</i>: <strong><i>{stock.get('Previous Week Mean Forecast')}</i></strong></p>-->
                        <h2>Demand Forecast and Issues Analysis <i class="new">(New!)</i></h2>
                        <table>
                        <thead>
                            <tr>
                                <th>Current Week Forecast</th>
                                <th>Actual Units Issued</th>
                                <th>Forecast Accuracy (%)</th>
                                <th>Upcoming Week Forecast</th>
                                <th>Upcoming Week 95% CI Forecast</th>
                            </tr>
                        </thead>
                        <tbody>
                            <!-- Example row -->
                            <tr>
                              
                                <td>{stock.get('Current Week Forecast')}</td>
                                <td>{stock.get('Actual Units Issued')}</td>
                                <td>{stock.get('Forecast Accuracy (%)')}</td>
                                <td>{stock.get('Upcoming Week Forecast')}</td>
                                <td>{stock.get('Upcoming Week 95% CI Forecast')}</td>
                            </tr>
                            <!-- Add more rows as needed -->
                        </tbody>
                      </table>

                        <hr>
                      </body>
                    </html>
                    """

                    details.append(info)
                else:
                    info = f"""
                    <html>
                      <head>
                        <style>
                          body {{
                            font-family: Arial, sans-serif;
                            font-size: 16px;
                            line-height: 1.6;
                            color: #333;
                            margin: 0;
                            padding: 20px;
                          }}
                          h1, h2, h3, h4, h5, h6 {{
                            font-weight: bold;
                          }}
                          p {{
                            margin: 0 0 20px;
                          }}
                          strong {{
                            font-weight: bold;
                          }}
                          hr {{
                            border: none;
                            border-top: 1px solid #ccc;
                            margin: 20px 0;
                          }}
                        </style>
                      </head>
                      <body>
                        <h1>{stock.get('Stock Name')}</h1>
                        <p>Base Cost Price: ₦{stock.get('Base Cost Price')}</p>
                        <p>Previous Cost Price: ₦{stock.get('Prev Cost Price')}</p>
                        <p>Current Cost Price: ₦{stock.get('Current Cost Price')}</p>
                        <p>Percentage Change: <strong>{stock.get('Percentage_Change')}</strong></p>
                        <hr>
                      </body>
                    </html>
                    """

                    details.append(info)


            description = "\n".join(details)

            subject = "Urgent Notification: Inventory Price Change Alert"

            body = f'''
                                <html>
                                  <body>
                                  <p>Esteemed Management Team,</p>
<p>Warm greetings to each of you.</p>
<p>I hope this message finds you in good spirits and ready to take on new challenges.</p>
<p>I am reaching out to discuss the recent movements in our stock cost prices.It's essential to keep you informed about these developments.</p>
<p>Attached, you'll find a detailed overview detailing the particulars of the recent fluctuations in prices:</p>
                                  <body/>
                                <html/>

                                {description}

                                <html>
                                  <body>
                                    <br/>
                                    <h3>Disclaimer for Forecast Report</h3>
                                    <p><strong>
                                    The foregoing forecast report for inventory and demand analysis has been generated using advanced predictive analytics techniques, including machine learning models. While these forecasts are based on historical data and sophisticated 
                                    algorithms, they are inherently subject to uncertainty and should be used as a guide rather than an absolute prediction.
                                    </strong>
                                    </p>
                                    <br/>
                                    <p>Best Regards,<p/>
                                    <p>Kenneth Mark,<p/>
                                    <p>Store-Keeper<p/>
                                    <br/>
                                    <p>For more, visit <a href="https://docs.google.com/spreadsheets/d/1UgWKrR8G_4FISCLrY4aeR9lLRdrDtkhPFKJkkUHczQ0/edit?usp=sharing">Our Inventory List</a>.</p>
                                  </body>
                                </html>
                                '''
            msg = MIMEMultipart()
            msg.attach(MIMEText(body, "html", "utf-8"))

            msg["Subject"] = subject
            msg["From"] = username

            con = smtplib.SMTP_SSL("smtp.gmail.com")

            con.login(username, password)

            msg['To'] = ', '.join(receiver_mail_email_list)  # Join email addresses with commas
            con.sendmail(username, receiver_mail_email_list, msg.as_string())

            con.quit()
            print("Email was sent Successfully.")

    def update_databases_with_new_stock(self):
        """
        This method updates the base and previous databases with new stocks
        :return:
        """
        print("Fetching Current Stock Database....")
        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("CURRENT_COST_PRICE_API_KEY"))
        cur_worksheet = sheet.worksheet("Current Costs")
        current_cost_record_list = cur_worksheet.get_all_values()

        current_cost_df = pd.DataFrame(data=current_cost_record_list[1:], columns=current_cost_record_list[0])
        current_cost_df["Cost price"] = current_cost_df["Cost price"].str.replace(",", "").astype(float)
        #current_cost_df = current_cost_df[["Stock name", "Cost price"]]
        current_stock_name_list = current_cost_df["Stock name"].unique().tolist()
        current_stock_dict = current_cost_df.to_dict(orient="records")


        print("Fetch was Successful!")
        print()


        print("Updating The Base Database with Possible New Stock....")
        # a) Getting the Base Cost Db:

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("BASE_COST_PRICE_API_KEY"))
        base_worksheet = sheet.worksheet("Base Cost")
        base_cost_record_list = base_worksheet.get_all_values()

        base_cost_df = pd.DataFrame(data=base_cost_record_list[1:], columns=base_cost_record_list[0])
        base_cost_df["Cost price"] = base_cost_df["Cost price"].str.replace(",", "").astype(float)
        #base_cost_df = base_cost_df[["Stock name", "Cost price"]]
        base_stock_name_list = base_cost_df["Stock name"].unique().tolist()

        base_stock_dict = base_cost_df.to_dict(orient="records")
        for item,curr_item_dict in zip(current_stock_name_list,current_stock_dict):
            if item not in base_stock_name_list:
                new_item_dict = dict()
                new_item_dict["Stock name"]=item
                new_item_dict["Qty"] = curr_item_dict["Qty"]
                new_item_dict["Cost price"] = curr_item_dict["Cost price"]
                new_item_dict["Amount"] = curr_item_dict["Amount"]

                base_stock_dict.append(new_item_dict)

                #print(new_item_dict)# 'CLING FILM (45cm wide)'
        new_df = pd.DataFrame(base_stock_dict)
        new_df.sort_values(by="Stock name",inplace=True)

        base_google_sheet_columns = new_df.columns.tolist()
        base_google_sheet_data = new_df.values.tolist()

        base_worksheet.clear()
        base_worksheet.append_rows([base_google_sheet_columns])
        base_worksheet.append_rows(base_google_sheet_data)
        print("Update Was Successfull")

        print("Updating The Previous Database with Possible New Stock....")
        time.sleep(1)
        # b) Getting the Previous Cost Db:

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        sheet = account.open_by_key(os.getenv("PREV_COST_PRICE_API_KEY"))
        prev_worksheet = sheet.worksheet("Previous Costs")
        previous_record_list = prev_worksheet.get_all_values()

        previous_cost_df = pd.DataFrame(data=previous_record_list[1:], columns=previous_record_list[0])
        previous_cost_df["Cost price"] = previous_cost_df["Cost price"].str.replace(",", "").astype(float)

        previous_stock_name_list = previous_cost_df["Stock name"].unique().tolist()

        previous_stock_dict = previous_cost_df.to_dict(orient="records")
        for item, pre_item_dict in zip(current_stock_name_list, previous_stock_dict):
            if item not in previous_stock_name_list:
                new_item_dict = dict()
                new_item_dict["Stock name"] = item
                new_item_dict["Qty"] = pre_item_dict["Qty"]
                new_item_dict["Cost price"] = pre_item_dict["Cost price"]
                new_item_dict["Amount"] = pre_item_dict["Amount"]

                previous_stock_dict.append(new_item_dict)

        new_df = pd.DataFrame(previous_stock_dict)

        new_df.sort_values(by="Stock name", inplace=True)

        prev_google_sheet_columns = new_df.columns.tolist()
        prev_google_sheet_data = new_df.values.tolist()

        prev_worksheet.clear()
        prev_worksheet.append_rows([prev_google_sheet_columns])
        prev_worksheet.append_rows(prev_google_sheet_data)

        print("Update Was Successfull")
        print()

        print("Updating The Weekly Change Database with Possible New Stock....")
        # a) Getting the Weekly change Cost Db:

        account = gc.service_account(os.getenv("STEAM_TALENT_JSON_FILE"))
        weekly_change_sheet = account.open_by_key(os.getenv("WEEKLY_DB_UPDATE_API_KEY"))
        weekly_change_worksheet = weekly_change_sheet.worksheet("Weekly Changes")
        weekly_change_record_list = weekly_change_worksheet.get_all_values()

        weekly_change_df = pd.DataFrame(data=weekly_change_record_list[1:], columns=weekly_change_record_list[0])
        weekly_change_df["Base Rate"] = weekly_change_df["Base Rate"].str.replace(",", "").astype(float)
        # base_cost_df = base_cost_df[["Stock name", "Cost price"]]
        weekly_change_stock_name_list = weekly_change_df["Stock Name"].unique().tolist()

        weekly_change_stock_dict = weekly_change_df.to_dict(orient="records")
        for item, curr_item_dict in zip(current_stock_name_list, current_stock_dict):
            if item not in weekly_change_stock_name_list:
                new_item_dict = dict()
                new_item_dict["Stock Name"] = item
                new_item_dict["Base Rate"] = curr_item_dict["Cost price"]
                weekly_change_stock_dict.append(new_item_dict)

        new_df = pd.DataFrame(weekly_change_stock_dict)
        new_df.sort_values(by="Stock Name", inplace=True)

        weekly_change_google_sheet_columns = new_df.columns.tolist()
        weekly_change_google_sheet_data = new_df.values.tolist()
        weekly_change_google_sheet_data = [[str(data) for data in record] for record in weekly_change_google_sheet_data]

        weekly_change_worksheet.clear()
        weekly_change_worksheet.append_rows([weekly_change_google_sheet_columns])
        weekly_change_worksheet.append_rows(weekly_change_google_sheet_data)
        print("Update Was Successfull")

    def execute_work_flow(self):
        self.update_databases_with_new_stock()

        print("Computing Relevant Items")
        relevant_items = self.get_most_relevant_stock()
        print("Relevant Items Successfully Computed")
        print()

        print("Processing for Cost Parameters")
        time.sleep(2)
        df = self.process_for_base_previous_current_costs(relevant_items)
        print("Done Processing Cost Parameters")

        time.sleep(2)
        sig_items = self.check_inventory_db_for_price_change_significance(df[0])
        print(sig_items)
        

        # self.compute_moving_average_for_sig_items()

        # time.sleep(2)
        # self.update_base_cost_db_with_new_base_costs(sig_items)

        # time.sleep(2)
        # self.update_google_sheet_dashboard(df[1])

        # time.sleep(2)
        # self.update_previous_cost_db_with_current_cost()

        # time.sleep(2)
        # self.create_a_column_for_each_sig_price_change(sig_items)

        # time.sleep(2)
        # self.sent_email(sig_items)

if __name__ == '__main__':
    tracker = PriceTrackingAutomator()
    tracker.execute_work_flow()
    #print(tracker.get_all_purchases())