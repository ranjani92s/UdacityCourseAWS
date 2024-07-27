Attached all the requested files and screenshots

The count was as expected 
In Landing Zone
  Customer: 956
  Accelerometer: 81273
  Step Trainer: 28680
In Trusted Zone
  Customer: 482
  Accelerometer: 40981
  Step Trainer: 14460
In Curated Zone
  Customer: 482
  Machine Learning: 43681

  Tables and columns 

**Customer Landing**
serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate
customername
email
lastupdatedate
phone
sharewithfriendsasofdate
Step Trainer Landing
sensorReadingTime
serialNumber
distanceFromObject
Accelerometer Landing
timeStamp
user
x
y
z

**Customer Trusted**
serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate is not null 
customername
email
lastupdatedate
phone
sharewithfriendsasofdate

**Customer Curated**( Customer Trsuted joins Accelerometer Landing on email == user)
serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate is not null 
customername
email
lastupdatedate
phone
sharewithfriendsasofdate

**Accelerometer Trusted** (Accelerometer Landing join customer_trsusted)
serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate 
customername
email
lastupdatedate
phone
sharewithfriendsasofdate
timeStamp
user
x
y
z

**Step trainer Trusted** (customer curated join step trainer landing)
registrationdate
sensorReadingTime
serialNumber
distanceFromObject

**Machine Learning Curated** (Step trainer Trusted join accelerometer trsuted)
registrationdate
sensorReadingTime
serialNumber
distanceFromObject
serialnumber
sharewithpublicasofdate
birthday
registrationdate
sharewithresearchasofdate 
customername
email
lastupdatedate
phone
sharewithfriendsasofdate
timeStamp
x
y
z
