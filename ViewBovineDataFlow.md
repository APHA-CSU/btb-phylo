# **Data flow for ViewBovine**
`btb-phlyo` deals with collating whole genome sequencing (WGS) samples for displaying in the ViewBovine APHA web app before processing WGS SNP data for the selected samples. 

Collating samples involves cleaning and filtering samples based on their metadata before cross-referencing with two other datasets (cattle and movement data) which are loaded into ViewBovine along with processed WGS SNP data.

The below the diagram illustrates how metadata pertainting to all samples is processed and collated from three sources: WGS (`all_wgs_samples.csv`), cattle (`cattle.csv`) and movement (`movement.csv`) in order to select samples for phylogeny.

Each .csv file in this diagram is output and saved in the results or metadata folders for each run of `btb-phylo`. The files in green are used directly by ViewBovine and contain metadata and snp data relating to the exact same final sample set.

<img src="https://user-images.githubusercontent.com/10742324/198301036-a8560670-ab7b-4e00-ba62-bbcf402d93d4.png" width="400">

First, the WGS sample metadata is processed: 
1. **Remove duplicates samples**: Any WGS submission may be sampled multiple times to achieve suffecient quality. The "de_duplicate" stage selects the best sample for each submission. 
2. **Filter low-quality samples**: Removing samples that are not of suffecient quality to be included in phylogeny for ViewBovine.

<img src="https://user-images.githubusercontent.com/10742324/198324132-04a3834b-5228-4afc-a1dc-7e1a23e99a98.PNG" width="800">

The below venn diagram illustrates how the resulting datasets relate to eachother. Notice that "filtered samples" (`passed_wgs.csv`) is a subset of "deduplicated samples" (`deduped_wgs.csv`) which is itself a subset of "all samples" (`all_wgs_samples.csv`).

<img src="https://user-images.githubusercontent.com/10742324/198075303-21798bf5-381d-4614-9f7d-addccdf4a109.PNG" width="500">

The next stage is to "consistify" the WGS, cattle and movement samples. There are a number of samples that do not exist in all three of these datasets. These samples must be removed because every sample in ViewBovine must have SNP (WGS), cattle and movement data. If any one or more of these data are missing this sample will fail. 

The consistify stage takes the `passed_wgs` samples, and `cattle` and `movement` samples as input and it outputs `consistified_wgs`, `constified_cattle` and `consistified_movement` samples. These are subsets of the original `wgs` `cattle` and `movement` sample sets and contain only samples that are common to all three datasets.

The below venn diagram illustrates how WGS, cattle and movement datasets relate to each other. The WGS samples show the `deduplicate` and `passed` subsets (see above venn diagram). The samples which are included in ViewBovine are the intersection of `passed_wgs`, `cattle` and `movement` samples.

<img src="https://user-images.githubusercontent.com/10742324/198075316-7acee140-45de-43c8-8da0-ca31873eacac.PNG" width="500">

We finally generate a "missing submission report", `report.csv`. This file contains metadata relating to all the submissions that have been exluded from ViewBovine. The intention of this file is to *i)* have a record of submissions that have been excluded from the ViewBovine and *ii)* understand the reason why any given submission has been excluded. 

The reasons why a submission might be excluded are either *i)* due to filtering; it may not have passed one of the filtering criteria *ii)* the sample is missing from one or more of the three key datasets (WGS, cattle or movement), i.e. it did not pass the consistify stage.

<img src="https://user-images.githubusercontent.com/10742324/198324636-0ab48e96-53a3-4f17-8c65-51a5272849f3.PNG" width="400">

**Note:** the `report.csv` does not contain information about duplciate WGS **samples**, i.e. it only relates to why any given **submission** is missing from ViewBovine. This is because users of the app are not concerned with particular WGS samples of a given submission, they are only concerned with the submission itself.   

The below venn diagram illustrates the set of submissions that is included in the missing submission report. Note that compared with the above venn diagram, the "ViewBovine samples" and "duplciate samples" have been removed.

<img src="https://user-images.githubusercontent.com/10742324/198075326-11d57aea-d314-43d8-baa9-64469832d906.PNG" width="500">

