# icohs-analysis 





##### DHIS2 API 

This repository contains python implementation of DHIS2 API for data download.

## Requirments & Run
```
python dhis_api.py
```

## How to use (Temporary)

The following variables should be changed in the code depending in your requirements. This is a temporary fix and will be automated:


1. dataElement name:  set a data element name you want to download data for. List of the data elements
```python
  data_element_name = '105-AN01a'     #----- code line 103 
  ```
  
2.  Start date and end date.
```python
    startDate= '2020-01-01'  #---------------- code line 121
    endDate= '2020-05-19'    # ---------------- code line 122
   ```

3. Loop through subsets of organization units if you are doing an initial dump of a long period of data.
```python
    org_group_list = org_group_list[100:150]   #--------------------- code line 116
  ```


