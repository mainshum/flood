# Floods 

## Goal
This exploration is meant to provide a novel insight into flood incidents in UK: we attempt to find regions with high degree of prevelence, look into common incident causes and - ultimately - use that as a basis for recommendations for municpalities.

## Data 
Data used as a basis for analysis was provided by sewer infrastructure companies across UK, following requests for environment information.  As stipulated by Environmental Information Regulations (EIR) this data is meant to be public and so all the sources can be viewed in the `/source` folder. The law outlines what information should be shared, but it imposes no strict rules as to how the data should be formatted and so, the first challenge is to standardize all sources. 

## Approach 
We meant for this research to be easily reproducible and provide all the steps we've taken to arrive at final results. Eventually, we also aim to format the whole thing as a single python notebook.

### Standardize data sources 

Some data sources come with dozens of columns and some with only a few. Given our research question and the fact we wanto to cover as many regions as possible (and thus leverage even poorer data spreadsheets), we opted to only extract the following columns: 

* `incident_date: str` 
* `incident_type: str`
* `location: Dict`

Run `process.py` in subfolder for each company in `/source` to create a `parquet` file in `/results` folder, containing data in standard form. 

### Find distinct incident types 

After processing data for all companies we're left with 300+ distinct incident types e.g:

| Incident Type                              | Count  |
|--------------------------------------------|-------:|
| Blockage                                  | 57452  |
| Blockage due to 3rd party                 |   223  |
| Collapse                                  |  2013  |
| Collapse due to 3rd party                 |    42  |
| Equipment Failure                         |  1405  |
| Equipment failure due to 3rd Party        |    28  |
| Overloaded                                |  5543  |
| Pumping Station Failure                   |  4227  |
| Pumping Station Failure due to 3rd party  |    31  |
| ...                                       | ... | 

The above table can be recreated by running `incident_type_counts.py`. It is not very revealing and unwieldy to work with so many incident types and so the next step is to map each `incident_type` onto one of 3 categories: 

#### Maintenance
Typical blockages in the main (fats/roots/wipes etc.)
Non-typical blockages in the main (construction materials, introduced items)
Pump station breakdown/outage

#### Asset Management (this could be read as under-investment in some cases)
Collapse/burst
Partial Collapse
Sewer Condition

#### Not fit for purpose (cannot take hydraulic loading)
Hydraulic overload (network based)
Hydraulic overload (pump station based - overflow at pump station)
Hydraulic overload (pump station caused - overflow at pump station  discharge) (I haven't seen this called out as a cause so this may be redundant if they aren't doing so).

The rationale behind this selection is to have "buckets" that are wide enough to cover majority of incident types and disconnected enough to easily decide which type belongs where. Additionally, these can be subsequently used to assist at deciding what steps should be taken to address and mitigate the most prevalent incident drivers in each region. The outcome of the aggregation - created with help of our domain expert and AI - can be found in `incidents.csv`

