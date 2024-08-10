import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

# Write directly to the app
st.title('🍹 Pending Smoothie Orders 🍹')
st.write("orders that need to be filled!")

# Get the active session
session = get_active_session()
# Fetch the data as a DataFrame
my_dataframe = session.table('smoothies.public.orders').filter(col("ORDER_FILLED") == False).to_pandas()


if my_dataframe:
    # Display the data for editing
    editable_df = st.data_editor(my_dataframe)
    # Submit button
    submitted = st.button('Submit')
    try:
        # Ensure editable_df is a DataFrame
        if not isinstance(editable_df, pd.DataFrame):
            editable_df = pd.DataFrame(editable_df)

        # Fill any NaN values with empty strings and convert all data types to strings
        editable_df = editable_df.fillna("").astype(str)

        # Create a temporary table to hold the edited data
        session.create_dataframe(editable_df).write.mode("overwrite").save_as_table("smoothies.public.temp_orders")

        # Perform the merge operation
        merge_query = """
        MERGE INTO smoothies.public.orders AS target
        USING smoothies.public.temp_orders AS source
        ON target.ORDER_UID = source.ORDER_UID
        WHEN MATCHED THEN
            UPDATE SET target.ORDER_FILLED = source.ORDER_FILLED
        """
        session.sql(merge_query).collect()
        st.success('Someone clicked the button. :tada:')

    except:
        st.success('Something went wrong')

else:
    st.success('There are no pending order right now',icon="🍹")
