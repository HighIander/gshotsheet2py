import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import time

class Metadata():
    def __init__(self,
                 sheet,
                 keyfile = 'google_access.json',
                 worksheet = 0,
                 writesheet = 2,
                 head = 2,
                 cache_time = 10
                ):
        
        self.debug = False
        
        self._scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
                        
        if keyfile != None:
            self.set_keyfile(keyfile)

        self._worksheet = worksheet
        self._writesheet = writesheet
        self._sheet = sheet
        self._cache_time = cache_time
            
        #if worksheet != None and writesheet == None:
        self.set_worksheet(worksheet)

        if sheet != None:
            self.set_sheet(sheet)


        if head != None:
            self.head = head
            
        self._lastUpdate = 0
        self.update()
            
    def set_head(self,head):
        if self._head != head: self._read_again = True
        self._head = head

    def set_keyfile(self,keyfile):
        self._creds = ServiceAccountCredentials.from_json_keyfile_name(keyfile, self._scope)
        
    def set_sheet(self,sheet):
        self._sheet = sheet
        self._get_sheet_instance()

    def set_worksheet(self,worksheet, writesheet=None):
        self._worksheet = worksheet
        if writesheet != None:
            self._writesheet = writesheet
        self._get_sheet_instance()
        
    def _get_sheet_instance(self, verbose = True):
        retry_count = 0
        while retry_count < 5:
            try:
                # authorize the clientsheet 
                client = gspread.authorize(self._creds)
                # get the instance of the Spreadsheet
                sheet = client.open(self._sheet)
                # get the required sheet of the Spreadsheet
                self._sheet_instance = sheet.get_worksheet(self._worksheet)
                if self._writesheet != None:
                    self._writesheet_instance = sheet.get_worksheet(self._writesheet)
                return self._sheet_instance
            except Exception as e:
                retry_count += 1
                if verbose: print("Error getting the google sheet instance!")
                time.sleep(2)
                #return None

    def update(self, verbose = True):
        if time.time()-self._lastUpdate > self._cache_time:
            try:
                #read google spreadsheet, if already open:
                # get all the records of the data
                records_data = self._sheet_instance.get_all_records(head=self.head)#[2:]
                records_data_write = self._writesheet_instance.get_all_records(head=1)#[2:]
            except Exception as e:
                # else open the spreadsheet first
                records_data = self._get_sheet_instance().get_all_records(head=self.head)#[2:]
                records_data_write = self._writesheet_instance.get_all_records(head=1)#[2:]
            # convert the json to dataframe

            self.records_df_raw = pd.DataFrame.from_dict(records_data)
            self.records_df_write_raw = pd.DataFrame.from_dict(records_data_write)

            records_data_pd = []
            for rec in records_data:
                if rec["run_number"] != "":
                    records_data_pd.append(rec)
            records_data_pd_write = []
            for rec in records_data_write:
                if rec["run_number"] != "":
                    records_data_pd_write.append(rec)

            self.records_df = pd.DataFrame.from_dict(records_data_pd)
            self.records_df_write = pd.DataFrame.from_dict(records_data_pd_write)

            self._lastUpdate = time.time()
        else:
            if verbose: print("Warning: Using cached data")
        
    def get_unit(self,key,write=False):
        if write:
            return self.records_df_write_raw.at[0,key]
        else:
            return self.records_df_raw.at[0,key]
        
    def write(self,key, data, run_number, python=True, new_key=False, overwrite=False, verbose = True):
        '''
           Write something to the logbook. 
           * By default it is written to separate workbook "python". If you set python = False, it is written to the shot sheet (CAREFUL!)
           * By default, if something already exists in the cell, it is not overwritten. Set overwrite = True to overwrite (CAREFUL!)
        '''
        #try:
        if python:
            ws = self._writesheet_instance
            df = self.records_df_write_raw
        else:
            ws = self._worksheet_instance
            df = self.records_df_raw

        # Create empty dataframe
        df_ = pd.DataFrame()

        try:
            row = np.where(df['run_number'] == run_number)[0][0]
        except:
            #row = np.where(df['run_number'] != '')[0][-1:][0]+1 # select first row after written rows
            row = run_number
            ws.update_cell(row+2,df.columns.get_loc("run_number")+1,str(run_number))
        
        #find col:
        try:
            col = df.columns.get_loc(key)
            # Check the cell content before writing
            cell_value = ws.cell(row + 2, col + 1).value
            if not cell_value or overwrite:  # Only write if the cell is empty
                ws.update_cell(row + 2, col + 1, str(data))
            else:
                if verbose: print(f"Cell at row {row + 2}, column {col + 1} is not empty, skipping write.")
        except KeyError:
            if new_key:
                # If the key (column) does not exist, append a new column
                new_col_idx = df.shape[1]  # Get the next available column index
                col = new_col_idx  # This will be the new column position
                ws.update_cell(1, col + 1, key)  # Add the key to the header row
                #update the first sheet with df, starting at cell B2. 
                ws.update_cell(row+2,col+1,str(data))
            else:
                if verbose: print(f"Error: Key does not exist ({key})!")


    def get(self,key, run_number, python = False, verbose = True):
        '''
        returns the value for a given key and run_number.
        defaults to the value of the run when last time a value was specified, if no value is given in the logbook for run_number
        '''
        self.update()
        runNr = run_number*1
        try:
            if python:
                df = self.records_df_write
            else:
                df = self.records_df
                
            #test for run:
            if df.set_index('run_number')["done"].to_dict()[runNr] == '': raise
             #(raises an error, if not existing, yet)
            while runNr>-10:
                try:
                    this = df.set_index('run_number')[key].to_dict()[runNr]
                except Exception as e:
                    this = ''
                if this != '': 
                    return this
                else:
                    runNr -= 1
            if verbose: print(f"Error: Value '{key}' not set for run_number {run_number}!")
            return "n/a"
        except Exception as e:
            if verbose: print("Error: run_number not found!")

    def get_all(self,python = False, verbose = False):
        if python:
            df = self.records_df_write
        else:
            df = self.records_df
        data = {}
        for r in df["run_number"]:
            run_data = {}
            for record in df:
                run_data[record] = self.get(record,r,python=python, verbose = verbose)
            data[r] = run_data
        return data