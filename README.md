# FIT3164-Backend

Clone the Directory https://github.com/SetyawanPrayogo/FIT3164-Backend.git

Create a new branch and make many commits

Make sure to git pull to stay up to date.


# To code for the frontend.
Comment out the following lines

in priceElasticityModel.py
line 67:     plt.show()

in app.py
if __name__ == '__main__':
    main()

the entire function for 
// Working code for backend only
"""
def main():   
    print("#############################################")
    ...

To work with the backend API. In the terminal, run. Preferably windows powershell and make sure you have all the packages installed. It is recommended to use Anaconda and install all the packages first in a separate environment.
>>> flask run

Once flask is running, you will be able to run the links on 127.0.0.1:5000.

If you want to use the full UI, download the frontend files from 
https://github.com/MahirAbrar/ds-frontend
Once you clone it, move into that folder
on the terminal type
>>>npm install
-- npm install has to be run just once and never again unless you decide to uninstall the application.
>>>npm start
---------------------------


# To code for the Backend.
in app.py
comment the entire function as well as the app.route
# http://127.0.0.1:5000/get-price-elasticity?storeId=st1Cal&itemId=FOODS_1_001&yearId=2015
@app.route('/get-price-elasticity', methods=['GET'])
def main():   

Comment the next line in app.py, this will be at the bottom of the code.
// for testing with frontend
if __name__ == '__main__':
    app.run(debug=True)

in priceElasticityModel.py
Uncomment line 67:     plt.show()

Make sure to uncomment everything commented from the frontend.
