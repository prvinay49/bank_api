import json
import traceback
from fastapi import FastAPI, Body
from fastapi.responses import JSONResponse
from schema import CustomerSchema
import logging
from fastapi.encoders import jsonable_encoder
from db import (
    does_account_exists_for_customer,
    create_customer_and_account,
    get_account_balance,
    get_account_details,
    account_helper,
    customer_helper,
    update_balance,
    get_customer_details
)

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)

app = FastAPI(
    title="Banking API",
    version="1.0.0",
    docs_url="/",
    redoc_url="/docs"
)


def ResponseModel(data, status_code, message):
    return {
        "data": [data],
        "code": status_code,
        "message": message,
    }


@app.post("/create_account", response_description="Acount created successfully")
async def create_account(account_holder_details: CustomerSchema = Body(...)):
    try:
        if await does_account_exists_for_customer(account_holder_details):
            return JSONResponse(status_code=409, content={
                "account_holder_details": jsonable_encoder(account_holder_details),
                "message": f"Customer already has account registered under Aadhaar {account_holder_details.aadhaar}"
            })
        logging.info("Creating Account")
        new_customer, new_account = await create_customer_and_account(jsonable_encoder(account_holder_details))
        logging.info("Created account")
        return JSONResponse(status_code=200, content={
            "account_holder_details": jsonable_encoder(new_customer),
            "account_details": jsonable_encoder(new_account),
            "message": f"Account has been created under Aadhaar {account_holder_details.aadhaar}"
        })
    except Exception as exception:
        logging.exception(f"Exception occured {exception}")
        traceback.print_exc()


@app.get("/get_account_details", response_description="Retrieved Account details")
async def get_details(account_number: str):
    try:
        account_exists, account = await get_account_details("account_number", account_number)
        if account_exists:
            logging.info("Getting account details from DB")
            aadhaar = account_helper(account)["aadhaar"]
            customer = await get_customer_details(aadhaar)
            return JSONResponse(status_code=200, content={
                "account_details": account_helper(account),
                "customer_details": customer_helper(customer)
            })
        return JSONResponse(status_code=404, content={
            "message": f"Account does not exist with account number {account_number}.Please recheck your account number."
        })
    except Exception as exception:
        logging.exception(f"Exception occured {exception}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={
            "message": f"Error occurred while trying to fetch the account details with account number {account_number}."
        })


@app.get("/get_account_balance", response_description="Retrieved account balance")
async def get_balance(account_number: str):
    try:
        logging.info("Getting account balance from DB")
        account_exists, balance = await get_account_balance(account_number)
        if account_exists:
            return JSONResponse(status_code=200, content=balance)
        return JSONResponse(status_code=404, content={
            "message": f"Account does not exist with account number {account_number}.Please recheck your account number."
        })
    except Exception as exception:
        logging.exception(f"Exception occured {exception}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={
            "message": f"Error occurred while trying to fetch the account balance with account number {account_number}."
        })


@app.post("/deposit_amount", response_description="Amout deposited")
async def deposit(account_number: str, amount: int):
    try:
        account_exists, account = await get_account_details("account_number", account_number)
        if account_exists:
            logging.info(f"Depositing {amount} to {account_number}")
            updated_account_balance = await update_balance(account_number, amount, account_helper(account)["balance"],
                                                           "DEPOSIT")
            logging.info(f"Deposited {amount} to {account_number}")
            return JSONResponse(
                status_code=200,
                content={
                    "updated_balance": updated_account_balance,
                    "message": f"{amount} deposited into your account",
                })
        elif not account["is_active"]:
            logging.info(f"Cannot deposit to de-activated account")
            return JSONResponse(
                status_code=200, content={
                    "message": f"Your account {account_number} is de-activated. Cannot deposit amount now."
                })
        return JSONResponse(
            status_code=404, content={
                "message": f"Account does not exist with account number {account_number}."
                           f"Please recheck your account number."
            })
    except Exception as exception:
        logging.exception(f"Exception occured {exception}")
        traceback.print_exc()
        return JSONResponse(status_code=500, content={
            "message": f"Error occurred while trying to deposit {amount} to the account with account number "
                       f"{account_number}."
        })

