# MAU and Signup data processor

## How to use

1. Make sure you have Python 3.10 installed
2. Install the requirements with `pip install -r requirements.txt`
3. Update the `.env` file with needed data
4. For MAU use `python mau.py <mau.csv>`
5. For Signup use `python signup.py <signup.csv>`
6. The output will be in the `output` folder

## Note

Because MAU data excel does have functions in it, you can't copy whole generated column as one. Otherwise the function in the excel will be overwritten!
