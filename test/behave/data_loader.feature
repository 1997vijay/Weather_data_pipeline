Feature: Download Files Functionality

  Scenario: Download specific file
    Given a URL "https://www1.ncdc.noaa.gov/pub/data/gsod/2023"
    And a connection string "NA"
    And cloud name "Azure"
    And a file output path "C:\Users\sg1386-dsk01-user1\Desktop\Clean Coding\Final_Project_Vijay_Kumar\data\raw"
    And saving to cloud is disabled
    And a file name "010010-99999-2023.op.gz"
    When files are downloaded
    Then the file "010010-99999-2023.op" should be saved locally at "C:\Users\sg1386-dsk01-user1\Desktop\Clean Coding\Final_Project_Vijay_Kumar\data\raw"

  Scenario: Download all files
    Given a URL "https://www1.ncdc.noaa.gov/pub/data/gsod/2023"
    And a connection string "NA"
    And cloud name "Azure"
    And a file output path "C:\Users\sg1386-dsk01-user1\Desktop\Clean Coding\Final_Project_Vijay_Kumar\data\raw"
    And saving to cloud is disabled
    And no specific file name
    When files are downloaded
    Then at least one file should be saved locally at "C:\Users\sg1386-dsk01-user1\Desktop\Clean Coding\Final_Project_Vijay_Kumar\data\raw"
