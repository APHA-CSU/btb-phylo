# **Data flow for ViewBovine**
`btb-phlyo` deals with collating whole genome sequencing (WGS) samples for displaying in the ViewBovine APHA web app before processing WGS SNP data for the selected samples. 

Collating samples involves cleaning and filtering samples based on their metadata before cross-referencing with two other datasets (cattle and movement data) which are loaded into ViewBovine along with processed WGS SNP data.

![wgs_samples_venn](https://user-images.githubusercontent.com/10742324/198075303-21798bf5-381d-4614-9f7d-addccdf4a109.PNG)
![all_samples_venn](https://user-images.githubusercontent.com/10742324/198075316-7acee140-45de-43c8-8da0-ca31873eacac.PNG)
![report_samples_venn](https://user-images.githubusercontent.com/10742324/198075326-11d57aea-d314-43d8-baa9-64469832d906.PNG)
