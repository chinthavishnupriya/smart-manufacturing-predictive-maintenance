import streamlit as st
from retriever import answer_question

st.set_page_config(
    page_title="Maintenance Knowledge Assistant",
    page_icon="🔧",
    layout="wide"
)

st.title("Predictive Maintenance Knowledge Assistant")
st.caption("Local document retrieval prototype — TF-IDF + cosine similarity")

st.warning(
    "Demonstration prototype. Sample guidance is not an official equipment "
    "manual. Follow manufacturer documentation and site safety procedures."
)

question = st.text_input(
    "Ask a maintenance question",
    placeholder="What should we check when vibration increases?"
)

if st.button("Search knowledge base", type="primary") and question.strip():
    result = answer_question(question)

    st.subheader("Retrieved guidance")
    st.markdown(result["answer"])

    st.subheader("Retrieved sources")
    if result["sources"]:
        st.dataframe(result["sources"], use_container_width=True)
    else:
        st.info("No matching source was found. Try different wording.")
