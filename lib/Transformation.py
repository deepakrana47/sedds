from pyspark.sql.functions import array, col, collect_list, current_timestamp, date_format, isnull, lit, struct, when

"""
Transformation function that transform the party address to required format
"""
def process_address(df):
    return df.select(
        col("party_id"),
        struct(
            lit("INSERT").alias("operation"),
            struct(
                col("address_line_1").alias("addressLine1"),
                col("address_line_2").alias("addressLine2"),
                col("city").alias("addressCity"),
                col("postal_code").alias("addressPostalCode"),
                col("country_of_address").alias("addressCountry"),
                col("address_start_date").alias("addressStartDate")
            ).alias("newValue")
        ).alias("partyAddress")
    )

"""
Transformation function that transform the party data to required format
"""
def process_parties(df):
    return df.select(
        col("party_id"),
        col("account_id"),
        struct(
            lit("INSERT").alias("operation"),
            col("party_id").alias("newValue")
        ).alias("partyIdentifier"),
        struct(
            lit("INSERT").alias("operation"),
            col("relation_type").alias("newValue")
        ).alias("partyRelationshipType"),
        struct(
            lit("INSERT").alias("operation"),
            col("relation_start_date").alias("newValue")
        ).alias("partyRelationStartDateTime")
    )

"""
Transformation function that transform the account data to required format
"""
def process_accounts(df):
    return df.select(
        col("account_id"),
        struct(
            lit("INSERT").alias("operation"),
            col("account_id").alias("newValue")
        ).alias("contractIdentifier"),
        struct(
            lit("INSERT").alias("operation"),
            col("source_sys").alias("newValue")
        ).alias("sourceSystemIdentifier"),
        struct(
            lit("INSERT").alias("operation"),
            col("account_start_date").alias("newValue")
        ).alias("contactStartDateTime"),
        struct(
            lit("INSERT").alias("operation"),
            array(
                when(
                    ~isnull("legal_title_1"),
                    struct(
                        lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
                        col("legal_title_1").alias("contractTitleLine")
                    )
                ),
                when(
                    ~isnull("legal_title_2"),
                    struct(
                        lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
                        col("legal_title_2").alias("contractTitleLine")
                    )
                )
            ).alias("newValue")
        ).alias("contractTitle"),
        struct(
            lit("INSERT").alias("operation"),
            struct(
                lit("tax_id_type").alias("taxIdType"),
                col("tax_id").alias("taxIdType")
            ).alias("newValue")
        ).alias("taxIdentifier"),
        struct(
            lit("INSERT").alias("operation"),
            col("branch_code").alias("newValue")
        ).alias("contractBranchCode"),
        struct(
            lit("INSERT").alias("operation"),
            col("country").alias("newValue")
        ).alias("contractCountry")
    )

"""
Transformation function that aggregate the parties for each account
"""
def aggregate_account_party_address(party_df, address_df):
    # Joint party and party address
    return party_df.join(address_df, on="party_id").groupby("account_id").agg(
        collect_list(struct(
            col("partyIdentifier"),
            col("partyRelationshipType"),
            col("partyRelationStartDateTime"),
            col("partyAddress")
        )).alias("partyRelations")
    )

"""
Transformation the add the header and get the required components together as final result
"""
def add_header(account_df, party_address_df):
    header_info = ("SBDL-Contract", 1, 0)
    return account_df.join(party_address_df, "account_id", "left_outer") \
        .select(
            struct(
                lit(header_info[0]).alias("eventType"),
                lit(header_info[1]).alias("majorSchemaVersion"),
                lit(header_info[2]).alias("minorSchemaVersion"),
                lit(date_format(current_timestamp(), "yyyy-MM-dd'T'HH:mm:ssZ")).alias("eventDateTime")
            ).alias("eventHeader"),
            array(
                struct(
                    lit(header_info[0]).alias("eventType"),
                    col("account_id").alias("newValue")
                )
            ).alias("keys"),
            struct(
                "contractIdentifier",
                "sourceSystemIdentifier",
                "contactStartDateTime",
                "contractTitle",
                "taxIdentifier",
                "contractBranchCode",
                "contractCountry",
                "partyRelations"
            ).alias("payload")
        )