"""
Streamlit upload component for the Money Manager dashboard.

Put this in the dashboard repository and call render_mmbak_uploader()
from the sidebar or an Import page.
"""

import streamlit as st
from mm_etl import import_mmbak


def render_mmbak_uploader():
    st.subheader("Money Manager Import")

    uploaded = st.file_uploader(
        "Upload Money Manager backup",
        type=["mmbak"],
        help="Upload the latest .mmbak backup exported from Money Manager.",
    )

    if uploaded is None:
        return

    if st.button("Import / Update Supabase", type="primary"):
        with st.spinner("Reading backup and updating Supabase..."):
            try:
                result = import_mmbak(uploaded)

                if result["status"] == "skipped":
                    st.info(
                        "This exact backup has already been imported. "
                        "No duplicate transactions were created."
                    )
                else:
                    st.success(
                        f"Import complete — {result['transactions']:,} source "
                        f"transactions processed."
                    )

                st.json(result)

                # Clear cached dashboard queries after a successful import.
                st.cache_data.clear()

                # Optional: rerun so the dashboard immediately reflects new data.
                st.rerun()

            except Exception as exc:
                st.error(f"Import failed: {exc}")
                st.exception(exc)
