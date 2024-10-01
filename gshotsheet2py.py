import gspread
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import time

class Shotsheet():
    def __init__(self,
                 sheet,
                 keyfile = 'google_access.json',
                 worksheet = 0,
                 writesheet = 1,
                 head = 2,
                 cache_time = 10
                ):
        """
        Initialize the Shotsheet object for accessing and interacting with Google Sheets.

        Parameters:
        ----------
        sheet : str
            The name of the Google Sheets file.
        keyfile : str, optional
            The path to the JSON keyfile for Google API authentication. Defaults to 'google_access.json'.
        worksheet : int, optional
            The index of the worksheet containing the shot sheet data. Defaults to 0 (the first worksheet).
        writesheet : int, optional
            The index of the worksheet where data will be written. Defaults to 1 (the second worksheet).
        head : int, optional
            The number of header lines in the sheet, where the first line contains unique keys and the second line contains units. Defaults to 2.
        cache_time : int, optional
            The time in seconds to cache the data before fetching new data from Google Sheets. Defaults to 10 seconds.

        Returns:
        ----------
        None
        """
        self._sheet = sheet
        self._scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
        self._creds = ServiceAccountCredentials.from_json_keyfile_name(keyfile, self._scope)
        self._cache_time = cache_time
            
        #if worksheet != None and writesheet == None:
        self._set_worksheet(worksheet,writesheet)

        self._head = head
            
        self._lastUpdate = 0

    def _set_worksheet(self,worksheet, writesheet=None):
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
                if verbose: 
                    print("Error getting the google sheet instance!")
                    if retry_count < 4: print("   will retry...")
                time.sleep(2)
                #return None

    def update(self, verbose = True):
        """
        Update the cached records by fetching the latest data from Google Sheets.

        Parameters:
        ----------
        verbose : bool, optional
            If True, print warnings if using cached data instead of fresh data. Defaults to True.

        Returns:
        ----------
        None

        Notes:
        ----------
        - Data will only be fetched if the cache time (since last update) has expired.
        - This method updates both the main shot sheet and the Python-specific write sheet.
        """
        if time.time()-self._lastUpdate > self._cache_time:
            try:
                #read google spreadsheet, if already open:
                # get all the records of the data
                records_data = self._sheet_instance.get_all_records(head=self._head)#[2:]
                records_data_write = self._writesheet_instance.get_all_records(head=1)#[2:]
            except Exception as e:
                # else open the spreadsheet first
                records_data = self._get_sheet_instance().get_all_records(head=self._head)#[2:]
                records_data_write = self._writesheet_instance.get_all_records(head=1)#[2:]
            # convert the json to dataframe

            self._records_raw = pd.DataFrame.from_dict(records_data)
            self._records_python_raw = pd.DataFrame.from_dict(records_data_write)

            records_data_pd = []
            for rec in records_data:
                if rec["run_number"] != "":
                    records_data_pd.append(rec)
            records_data_pd_write = []
            for rec in records_data_write:
                if rec["run_number"] != "":
                    records_data_pd_write.append(rec)

            self.records = pd.DataFrame.from_dict(records_data_pd)
            self.records_python = pd.DataFrame.from_dict(records_data_pd_write)

            self._lastUpdate = time.time()
        else:
            if verbose: print("Warning: Using cached data")
        
    def get_unit(self,key,write=False,verbose=True):
        """
        Retrieve the unit for a specific key from the Google Sheets data.

        Parameters:
        ----------
        key : str
            The key (column name) for which the unit is retrieved.
        write : bool, optional
            If True, retrieves the unit from the write sheet instead of the main shot sheet. Defaults to False.
        verbose : bool, optional
            If True, provide additional logging output. Defaults to True.

        Returns:
        ----------
        unit : str
            The unit (given in the row under the row containing the keys) associated with the specified key.
        """
        self.update(verbose=verbose)
        if write:
            return self._records_python_raw.at[0,key]
        else:
            return self._records_raw.at[0,key]
        
    def write(self,key, data, run_number, python=True, new_key=False, overwrite=False, verbose = True):
        """
        Write data to the Google Sheet for a specific run number and key.

        Parameters:
        ----------
        key : str
            The key (column name) where data will be written.
        data : str, int, float
            The data to write into the specified cell.
        run_number : int
            The run number indicating the row in which the data will be written.
        python : bool, optional
            If True, data is written to the Python-specific write sheet. If False, it is written to the main shot sheet. Defaults to True.
        new_key : bool, optional
            If True, allows adding a new key (column) to the sheet if it doesn't already exist. Defaults to False.
        overwrite : bool, optional
            If True, allows overwriting existing data in the cell. Defaults to False.
        verbose : bool, optional
            If True, provides additional logging output. Defaults to True.

        Returns:
        ----------
        None

        Raises:
        ----------
        KeyError : If the key does not exist and `new_key` is False.
        ValueError : If the field is not empty and overwrite is False
        """
        self.update(verbose=verbose)
        
        if python:
            ws = self._writesheet_instance
            df = self._records_python_raw
        else:
            ws = self._worksheet_instance
            df = self._records_raw

        # Create empty dataframe
        df_ = pd.DataFrame()

        try:
            row = np.where(df['run_number'] == run_number)[0][0]
        except:
            #row = np.where(df['run_number'] != '')[0][-1:][0]+1 # select first row after written rows
            row = run_number
            col = df.columns.get_loc("run_number")+1
            cell_value = ws.cell(row + 2, col).value
            if not cell_value or overwrite:  # Only write if the cell is empty
                ws.update_cell(row+2,col,str(run_number))
        
        #find col:
        try:
            col = df.columns.get_loc(key)
            # Check the cell content before writing
            cell_value = ws.cell(row + 2, col + 1).value
            if not cell_value or overwrite:  # Only write if the cell is empty
                ws.update_cell(row + 2, col + 1, str(data))
            else:
                raise ValueError
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
        except ValueError:
            print(f"Cell at row {row + 2}, column {col + 1} is not empty, skipping write.")

    def get(self,key, run_number, python = False, verbose = True):
        """
        Retrieve the value for a specific key and run number.

        Parameters:
        ----------
        key : str
            The key (column name) from which the value is retrieved.
        run_number : int
            The run number (row) for which the value is retrieved.
        python : bool, optional
            If True, retrieves the value from the Python-specific write sheet. If False, retrieves from the main shot sheet. Defaults to False.
        verbose : bool, optional
            If True, provides additional logging output. Defaults to True.

        Returns:
        ----------
        value : str, int, float
            The value corresponding to the specified key and run number.

        Raises:
        ----------
        Exception : If the run_number does not exist or the value for the key is not found.
        """
        class KeyError(Exception):
            """Custom exception for errors in the Shotsheet class."""
            pass
        
        self.update(verbose=verbose)
        runNr = run_number*1
        try:
            if python:
                df = self.records_python
            else:
                df = self.records
                
            #test for run:
            if not run_number in list(df["run_number"]): raise ValueError
             #(raises an error, if not existing, yet)
            while runNr>-10:
                try:
                    this = df.set_index('run_number')[key].to_dict()[runNr]
                except Exception as e:
                    this = ''
                if this != '' or "comment" in key: 
                    return this
                else:
                    runNr -= 1
            raise KeyError
        except KeyError:
            if verbose: print(f"Error: Value '{key}' not set for run_number {run_number}!")
            return "n/a"
        except ValueError:
            if verbose: print(f"Error: run_number {run_number} not found!")


    def get_all(self, python=False, filter={}, sort=None, verbose=False):
        """
        Retrieve filtered records from the dataset, with support for filtering by date, time, datetime, 
        number ranges, and exact matches (including lists of values).

        Parameters:
        ----------
        python : bool, optional
            If True, will retrieve data from the Python records. Defaults to False.
        filter : dict, optional
            A dictionary specifying the filters to apply. The keys are the column names (e.g., "Date", "Time", "DateTime"),
            and the values can be a single value, a list of values, or a range (slice). Defaults to an empty dict.
        sort : str, optional
            The column name by which to sort the data. Defaults to None.
        verbose : bool, optional
            If True, will print additional information during execution. Defaults to False.

        Filter Examples:
        ----------
        1. Filtering by Date (mm/dd/YYYY format):
            - Single Date:
              filter = {"Date": datetime.today()}
            - Date Range:
              filter = {"Date": slice(datetime("10/01/2023", format='%m/%d/%Y'), datetime("10/03/2023", format='%m/%d/%Y'))}
            - List of Dates:
              filter = {"Date": [datetime("10/01/2023", datetime("10/03/2023", datetime("10/03/2023"]}

        2. Filtering by Time (HH:mm format) (deprecated):
            - Time Range:
              filter = {"Time": slice(time(13, 0), time(14, 30))}  # Between 1:00 PM and 2:30 PM

        3. Filtering by DateTime (combines Date and Time):
            - DateTime Range:
              filter = {"DateTime": slice(datetime.now()- timedelta(hours=1), datetime.now())}  # From 11:00 PM on Oct 1 to 1:00 AM on Oct 2

        4. Filtering by Numeric Ranges:
            - Number Range:
              filter = {"some_numeric_column": slice(10, 20)}  # Values between 10 and 20 (inclusive of 10, exclusive of 20)

        5. Filtering by Strings (exact matches or list of matches):
            - Single Value:
              filter = {"shot_type": "mainshot"}  # Exact match for a single category
            - List of Values:
              filter = {"shot_type": ["preshot", "postshot"]}  # Match any of the specified categories

        Returns:
        ----------
        data : dict
            A dictionary of filtered records where each key is a `run_number` and the value is another dictionary
            containing the filtered data for that run number.
        """
        from datetime import datetime, time,timedelta
        self.update(verbose=verbose)
        
        if python:
            df = self.records_python.copy()  # Make a copy of the dataframe
        else:
            df = self.records.copy()  # Make a copy of the dataframe

        # Combine 'Date' and 'Time' into a virtual 'DateTime' column if needed
        if "Date" in filter:
            # Convert 'Date' column (strings in "mm/dd/YYYY" format) to datetime objects
            df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
        if "Time" in filter:
            # Convert 'Time' column (strings in "HH:mm" format) to time objects
            df['Time'] = pd.to_datetime(df['Time'], format='%H:%M').dt.time

        if "DateTime" in filter:
            # Create a combined 'DateTime' column
            df['DateTime'] = df.apply(lambda row: datetime.combine(row['Date'], row['Time']), axis=1)

            # Apply the filter for 'DateTime'
            value = filter["DateTime"]
            if isinstance(value, slice):
                # Range filter for 'DateTime' using slice
                df = df[(df['DateTime'] >= value.start) & (df['DateTime'] <= value.stop)]
            elif isinstance(value, list):
                # Exact match for a list of datetime values
                exact_datetimes = [pd.to_datetime(v) for v in value]
                df = df[df['DateTime'].isin(exact_datetimes)]
            else:
                # Exact match for a single datetime value
                exact_datetime = pd.to_datetime(value)
                df = df[df['DateTime'] == exact_datetime]

        # Apply other filters if provided
        for key, value in filter.items():
            if key == "Time" and key != "DateTime":
                # Apply time filter for 'Time' column
                if isinstance(value, slice):
                    # Range filter for time
                    df = df[(df['Time'] >= value.start) & (df['Time'] < value.stop)]
                elif isinstance(value, list):
                    # Exact match for a list of times
                    exact_times = [pd.to_datetime(t, format='%H:%M').time() for t in value]
                    df = df[df['Time'].isin(exact_times)]
                else:
                    # Exact match for a single time value
                    exact_time = pd.to_datetime(value, format='%H:%M').time()
                    df = df[df['Time'] == exact_time]

            elif key == "Date" and key != "DateTime":
                # Apply date filter for 'Date' column
                if isinstance(value, slice):
                    # Range filter for date
                    df = df[(df['Date'] >= value.start) & (df['Date'] <= value.stop)]
                elif isinstance(value, list):
                    # Exact match for a list of dates
                    exact_dates = [pd.to_datetime(d, format='%m/%d/%Y') for d in value]
                    df = df[df['Date'].isin(exact_dates)]
                else:
                    # Exact match for a single date value
                    exact_date = pd.to_datetime(value, format='%m/%d/%Y')
                    df = df[df['Date'] == exact_date]

            else:
                # General filter for other columns
                if key != "DateTime":
                    if isinstance(value, slice):
                        # Range filter for general key
                        df = df[(df[key] >= value.start) & (df[key] <= value.stop)]
                    elif isinstance(value, list):
                        # Exact match for a list of values
                        df = df[df[key].isin(value)]
                    else:
                        # Exact match for a single value
                        df = df[df[key] == value]

        # Sort the data if a sorting key is provided
        if sort:
            df = df.sort_values(by=sort)

        # Build the final data dictionary
        data = {}
        for r in df["run_number"]:
            run_data = {}
            for record in df:
                run_data[record] = self.get(record, r, python=python, verbose=verbose)
            data[r] = run_data

        return data
