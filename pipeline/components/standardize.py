from pipeline.base import Component, DataWrapper
import numpy as np
import pandas as pd
import os
import csv

class Standardize(Component):
    """
    A helper class used by process.py to clean up and standardize raw data from retrieve.py, transforming
    things like column names, county names, etc. This class relies on pipeline/mappings/{STATE} to know 
    how different raw inputs map to our standaardized outputs.  
    """

    def __init__(self, name, state, date):
        """
        base_path resolves to the corresponding state folder that holds all required mappings. 
        For more information regarding col_map, col_lists, county_map, raw_county_list, and 
        master_county_list, refer to pipeline/mappings/guide.md
        """
        super().__init__(name)
        self.state = state
        self.date = date
        self.base_path = os.path.join("pipeline", "mappings", f"{self.state.upper()}")
        
        self.col_map = self.get_col_map()         
        self.col_lists = self.get_all_col_lists()   
        self.county_map = self.get_county_map()
        self.emc_county_map = self.get_emc_county_map()
        self.raw_county_list = self.get_raw_county_list()
        self.master_county_list = self.get_master_county_list()

    def get_col_map(self):
        """
        Expected format: {raw column name : standardized column name}
        """

        col_map = {}
        read_path = os.path.join(self.base_path, "col_map.csv")
        
        try:
            with open(read_path) as f:
                reader = csv.reader(f)
                header = next(reader)
                
                for row in reader:
                    if len(row) >= 2:
                        raw = row[0].strip().lower()
                        std_name = row[1].strip().lower()
                        col_map[raw] = std_name
        except FileNotFoundError:
            print(f"Warning: col_map.csv not found at {read_path}")
        
        return col_map

    def get_all_col_lists(self):
        """
        Expected format: {standardized col name : [raw col 1, raw col 2, ...]}
        """
        
        read_path = os.path.join(self.base_path, "raw_col_lists.csv")
        
        try:
            df = pd.read_csv(read_path)
            col_lists = {col: df[col].dropna().tolist() for col in df.columns}
        except FileNotFoundError:
            print(f"Warning: raw_col_lists.csv not found at {read_path}")
            col_lists = {}
        
        return col_lists

    def get_county_map(self):
        """
        Expected format: {raw county name : standardized county name}
        """

        county_map = {}
        read_path = os.path.join(self.base_path, "county_map.csv")
        
        try:
            with open(read_path) as f:
                reader = csv.reader(f)
                header = next(reader)
                
                for row in reader:
                    if len(row) >= 2:
                        raw = row[0].strip().lower()
                        std_name = row[1].strip().lower()
                        county_map[raw] = std_name
        except FileNotFoundError:
            print(f"Warning: county_map.csv not found at {read_path}")
        
        return county_map

    def get_emc_county_map(self):
        """
        Provider-specific county mappings, used when a provider reports towns or neighborhoods whose
        names collide with other places (e.g. the Town of Orange in Schuyler County vs Orange County).
        Read from rows of county_map_verified.csv that have a value in its optional 'emc' column.
        Preprocess never writes that file, so these hand-checked rows survive a rerun.

        Expected format: {(emc, raw place name) : standardized county name}
        """

        emc_map = {}
        read_path = os.path.join(self.base_path, "county_map_verified.csv")

        try:
            with open(read_path) as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames or "emc" not in reader.fieldnames:
                    return emc_map

                for row in reader:
                    emc = (row.get("emc") or "").strip().lower()
                    raw = (row.get("raw") or "").strip().lower()
                    std_name = (row.get("standard") or "").strip().lower()
                    if emc and raw and std_name:
                        emc_map[(emc, raw)] = std_name
        except FileNotFoundError:
            pass

        return emc_map

    def get_raw_county_list(self):
        """
        Expected format: [county 1, county 2, ...]
        """

        raw_county_list = set()
        read_path = os.path.join(self.base_path, "raw_county_list.txt")
        
        try:
            with open(read_path) as f:
                for line in f:
                    cleaned = line.strip().lower()
                    if cleaned:
                        raw_county_list.add(cleaned)
        except FileNotFoundError:
            print(f"Warning: raw_county_list.txt not found at {read_path}")
        
        return raw_county_list

    def get_master_county_list(self):
        """
        Expected format: [county 1, county 2, ...]
        """

        master_county_list = set()
        read_path = os.path.join(self.base_path, "master_county_list.txt")
        
        try:
            with open(read_path) as f:
                for line in f:
                    cleaned = line.strip().lower().replace("county", "").strip()
                    if cleaned:
                        master_county_list.add(cleaned)
        except FileNotFoundError:
            print(f"Warning: master_county_list.txt not found at {read_path}")
        
        return master_county_list

    def rename_cols(self, data):
        """
        Standardizes column names and drops unnecessary columns by:
            1) Lower casing column names and removing padding 
            2) Renaming columns to their standardized version
            3) Dropping unused columns
            4) Resolving duplicate columns by keeping the first non null value 
        """

        # Normalize column names
        normalized = {col: col.strip().lower() for col in data.columns}
        data.rename(columns=normalized, inplace=True)

        # Rename columns 
        data.rename(columns=self.col_map, inplace=True)

        # Drop unecessary columns
        allowed = set(self.col_map.values()) | {"% out", "emc"}
        drop_cols = [c for c in data.columns if c not in allowed]
        data.drop(columns=drop_cols, inplace=True)
        
        # Handle duplicate columns
        dupes = data.columns[data.columns.duplicated()].unique()

        for col in dupes:
            cols = [c for c in data.columns if c == col]

            # Extract duplicate columns as a small DataFrame
            block = data[cols]

            # Pick the first valid value
            merged = block.apply(
                lambda row: next(
                    (v for v in row 
                    if pd.notna(v) and str(v).strip() != ""),
                    None
                ),
                axis=1
            )

            data[cols[0]] = merged.to_numpy()
        
        data = data.loc[:, ~data.columns.duplicated(keep="first")]
        return data

    # Handles back, missing, and unknown data
    def handle_bad_data(self, data):
        """
        Bad data is classified as null, empty, or unknown county values and missing customers served values.

        For bad county data, we drop those values. For missing customers served values, we require a '% out'
        and total number affected columns to estimate the number of customers served as affected / % out. 
        When providers give values with the prefix '<', we strip the '<' and treat it as an exact value. However,
        more robust methods may be required.
        """

        # Drop NULL / unknown / empty rows
        data = data.dropna(subset=['county'])
        data = data[~data['county'].isin(['unknown', 'Unknown', 'UNKNOWN', ''])].copy()

        data.columns = data.columns.str.strip().str.lower()
        
        # If customers served data is missing, estimate it using affected / % out
        if "% out" in data.columns:
            if "customers_served" in data.columns:
                data.drop(columns="% out", inplace=True)
            else:
                # TODO: figure out how to handle "<" more robustly
                # For a county on a given day, if all % outs are "<" estimations, pull from historical
                # If a county has at least 1 "valid" % out entry then use that for the rest of its timestamps for that day

                # FOR LATER USE (WIP)
                mask = ~data['% out'].astype(str).str.contains('<', na=True)
                valid_rows = data[mask]
                
                # Standardize "< xx.xx%" to "xx.xx"
                data['% out'] = (
                    data['% out'].astype(str)
                    .str.replace('<', '', regex=False)
                    .str.rstrip('%')
                    .replace('', np.nan)
                )
                # Convert decimal to percent
                data['% out'] = pd.to_numeric(data['% out'], errors='coerce') / 100
                # Calculate esimated customers served
                data['customers_served'] = np.where(
                    (data['% out'].notna()) & (data['% out'] != 0),
                    (data['per_outage_customers_affected'].astype(float) / data['% out']).round().astype("Int64"),
                    np.nan
                )
                # Remove % out column as it is no longer needed
                data.drop(columns=['% out'], inplace=True)

        return data

    def standardize_data_types(self, data):
        """
        Filters the data to only keep rows whose timestamp match self.date, then standardizes data types:
            - timestamp: pandas datetime
            - per_outage_customers_affected: numeric
            - customers_served: numeric
        """

        data['timestamp'] = pd.to_datetime(data['timestamp'], errors='coerce')
        data = data[data['timestamp'].dt.date == pd.to_datetime(self.date).date()]

        data['per_outage_customers_affected'] = (
            pd.to_numeric(
                data['per_outage_customers_affected']
                .astype(str)
                .str.replace('"', '', regex=False)
                .str.replace(',', '', regex=False),
                errors='coerce'
            ).fillna(0)
        )
        
        data['customers_served'] = (
            pd.to_numeric(
                data['customers_served']
                .astype(str)
                .str.replace('"', '', regex=False)
                .str.replace(',', '', regex=False),
                errors='coerce'
            ).fillna(0)
        )
        
        return data

    def standardize_county(self, data):
        """
        Maps the raw county name values to the standardized county name. A regex is used to transform 
        certain formats to fit our mappings.
            e.g. 'ga-fulton' becomes 'ga - fulton'
        """

        data = data.copy()

        cs = (
            data['county']
            .astype(str)
            .str.strip()
            .str.lower()
        )

        cs = cs.str.replace(r'^([a-z]{2})-', r'\1 - ', regex=True)
        general = cs.map(self.county_map)

        # Provider-specific mappings take precedence over the general map
        if self.emc_county_map and 'emc' in data.columns:
            emcs = data['emc'].astype(str).str.strip().str.lower()
            specific = pd.Series(
                [self.emc_county_map.get((e, c)) for e, c in zip(emcs, cs)],
                index=data.index,
                dtype=object,
            )
            data['county'] = specific.where(specific.notna(), general)
        else:
            data['county'] = general

        # Standardize datatype
        data = data[data['county'].map(type) == str]
        
        return data

    def standardize(self, data):
        """
        Entry point fof the class. Takes in as inputs:
            - data: a single dataframe with the combined raw data
        
        Sequence of operations:
            1)  Checks for and handles multiple customers affected columns, keeping the one with the 
                highest values
            2)  Renames columns and handles bad data
            3)  Ensures all required columns exists, skipping providers with missing columns
            4)  Standardizes data types and county names

        Returns:
            [True, standardized_df] on success or [False, list_of_missing_columns] on failure.
            On success, the pipeline continues execution, and processes the data. On failure,
            the pipeline stops execution and logs the error.
        """

        # Check for multiple customers affected columns
        if 'c_affected' in self.col_lists:
            dupes = [c for c in data.columns 
                    if c.strip().lower() in self.col_lists['c_affected']]
            
            if len(dupes) > 1:
                # If duplicate affected columns exist, keep the one with higher values
                keep = data[dupes].sum(skipna=True).idxmax()
                data.drop(
                    columns=[c for c in data.columns if c != keep and c in dupes],
                    inplace=True
                )
                
        renamed_data = self.rename_cols(data)   
        good_data = self.handle_bad_data(renamed_data)

        # Check for if any columns are still missing. If so, skip that provider
        std_names = ['county', 'per_outage_customers_affected', 'customers_served', 'timestamp']
        missing = [name for name in std_names if name not in good_data.columns]
        
        if missing:
            return [False, missing]
        
        # Standardize data types
        type_std_data = self.standardize_data_types(good_data)
        type_std_data = type_std_data[type_std_data['customers_served'] != 0]

        # Standardize county names
        std_data = self.standardize_county(type_std_data)

        # Roll town/neighborhood-level providers up to one row per county per timestamp
        std_data = self.aggregate_town_level(std_data)

        return [True, std_data]

    def get_town_level_providers(self):
        """
        Providers (emc values) that report by town or neighborhood instead of by county, listed one per
        line in mappings/{STATE}/town_level_providers.txt. Optional: states without the file are unaffected.
        """

        read_path = os.path.join(self.base_path, "town_level_providers.txt")
        try:
            with open(read_path) as f:
                return {line.strip().lower() for line in f if line.strip() and not line.startswith("#")}
        except FileNotFoundError:
            return set()

    def get_served_baseline(self):
        """
        County customers served per provider from historicalCustomersServed/{STATE}_customers_served.csv,
        when that file has an 'emc' column.

        Expected format: {(county, emc) : customers served}
        """

        read_path = os.path.join("pipeline", "historicalCustomersServed", f"{self.state.upper()}_customers_served.csv")
        try:
            df = pd.read_csv(read_path)
        except FileNotFoundError:
            return {}
        if "emc" not in df.columns:
            return {}

        df = df.dropna(subset=["emc"])
        return {
            (str(c).strip().lower(), str(e).strip().lower()): float(v)
            for c, e, v in zip(df["county"], df["emc"], df["customers_served"])
        }

    def aggregate_town_level(self, data):
        """
        For providers that report by town or neighborhood, each row only covers part of a county. Left as is,
        the pipeline would treat every town as the whole county: customers served would be the largest single
        town, and outages in different towns would be read as one changing count.

        This sums customers affected across a county's towns for each timestamp, giving the same one row per
        county per timestamp that county-level providers report. Customers served is taken from the provider's
        county baseline (all of its towns, not only those with outages); without a baseline it falls back to the
        sum of the towns reported at that timestamp.
        """

        providers = self.get_town_level_providers()
        if not providers or data.empty or "emc" not in data.columns:
            return data

        emcs = data["emc"].astype(str).str.strip().str.lower()
        mask = emcs.isin(providers)
        if not mask.any():
            return data

        town = data[mask].assign(emc=emcs[mask])
        rolled = (
            town.groupby(["county", "emc", "timestamp"], as_index=False)
            .agg(
                per_outage_customers_affected=("per_outage_customers_affected", "sum"),
                customers_served=("customers_served", "sum"),
            )
        )

        baseline = self.get_served_baseline()
        if baseline:
            base = pd.Series(
                [baseline.get((c, e)) for c, e in zip(rolled["county"], rolled["emc"])],
                index=rolled.index,
                dtype=float,
            )
            rolled["customers_served"] = base.where(base.notna(), rolled["customers_served"])

        return pd.concat([data[~mask], rolled], ignore_index=True)

    def run(self, data):
        """
        The public interface inherited from Component. Receives data in the form of a DataWrapper and 
        returns the standardized data in DataWrapper form.
        """
        normalized_data = self.standardize(data)
        provider_data_std = DataWrapper(normalized_data)
        return provider_data_std