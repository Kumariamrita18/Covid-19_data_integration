# Hot to run the project

## Install dependencies
```
pip install -r requirements.txt
```

## Run the project

### Run fastapi server

```
uvicorn app:app --reload
```

### Initiate the database

```
python init_database.py
```

### Run streamlit app

```
streamlit run streamlit_app.py
```