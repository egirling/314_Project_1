# 314_Project_1
Project 1 Repo


Scenario: If person was put into cryosleep they were transported to another dimension 

    > Given Bob was put into cryosleep 
    > When We generate list of people that are transported to another dimension
    > Then Bob is on that list
    
Scenario: If the person is a child they were not transported

    > Given Billy Bob is child
    > When We generate list of people that are transported
    > Then Billy Bob is not on that list
    
Scenario: If a person is on the starboard side of the ship they were transported to another dimension

    > Given Marla was on the starboard side of the ship
    > When We generate a list of people that are transported
    > Then Marla is on that list


Work Distribution:

Tracy: wrote `test_remove_NA`and `test_splitWomanAndChildrenFromMen`, 
    made adjustments to `test_splitCabin`, `test_sumAmenityCharges`, `test_splitByVIP`

Heidi: wrote `test_splitCabin`, `test_sumAmenityCharges`, `test_splitByVIP`

Nicole: wrote `splitCabin`

Elizabeth: wrote `splitWomanAndChildrenFromMen`, `sumAmenityCharges`, `splitByVIP`