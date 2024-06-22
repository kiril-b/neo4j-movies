import ast
import json
import logging
import os
from pathlib import Path

import dotenv
import pandas as pd
from tqdm.auto import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm  # Import for logging

dotenv.load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)

tqdm.pandas()


def load_and_dump_json(x: str) -> str:
    list_of_dicts = ast.literal_eval(x)
    return json.dumps(list_of_dicts)


def move_movie_id_in_jsons(row: pd.Series) -> pd.Series:
    cast, crew, movie_id = (
        ast.literal_eval(row["cast"])[:5],
        ast.literal_eval(row["crew"])[:3],
        row["id"],
    )

    for c in cast:
        c["movie_id"] = movie_id

    for c in crew:
        c["movie_id"] = movie_id

    return pd.Series([cast, crew], index=["cast", "crew"])


def main() -> None:
    logger.info("Data preprocessing has started... 🤓")
    data_path = Path(os.environ.get("DATA_PATH", None))

    if not data_path.exists():
        raise ValueError(f"Invalid path to data: {str(data_path)}")

    if not (data_path / "credits.csv").exists():
        raise ValueError(
            "Your data folder does not contain the initial files needed for preprocessing!"
        )

    # if the files are already created, just return
    if all(
        (data_path / f).exists()
        for f in [
            "cast_info.csv",
            "cast_movie_relationship.csv",
            "crew_info.csv",
            "crew_movie_relationship.csv",
            "movies.csv",
        ]
    ):
        logger.info("Your files are already processed! Exiting... 🙋‍♂️")
        return

    # process credits.csv
    df = pd.read_csv(data_path / "credits.csv")

    with logging_redirect_tqdm(loggers=[logger]):
        df = df.progress_apply(move_movie_id_in_jsons, axis=1)

    logger.info("Almost done... 😴")

    cast, crew = df["cast"], df["crew"]

    cast = cast[cast.map(lambda x: len(x) > 0)].reset_index(drop=True)
    crew = crew[crew.map(lambda x: len(x) > 0)].reset_index(drop=True)

    cast_df = pd.DataFrame(cast.explode().reset_index(drop=True).values.tolist())
    crew_df = pd.DataFrame(crew.explode().reset_index(drop=True).values.tolist())

    cast_info = (
        cast_df[["id", "name", "gender"]]
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )
    cast_movie_relationship = cast_df[["id", "movie_id", "character"]].rename(
        columns={"id": "actor_id"}
    )

    crew_info = (
        crew_df[["id", "name", "gender"]]
        .drop_duplicates(subset=["id"])
        .reset_index(drop=True)
    )
    crew_movie_relationship = crew_df[["id", "movie_id", "department"]].rename(
        columns={"id": "actor_id"}
    )
    crew_movie_relationship["department"] = crew_movie_relationship[
        "department"
    ].str.upper()

    cast_info.to_csv(data_path / "cast_info.csv", index=False)
    cast_movie_relationship.to_csv(
        data_path / "cast_movie_relationship.csv", index=False
    )
    crew_info.to_csv(data_path / "crew_info.csv", index=False)
    crew_movie_relationship.to_csv(
        data_path / "crew_movie_relationship.csv", index=False
    )

    del df
    del cast_info
    del cast_movie_relationship
    del crew_info
    del crew_movie_relationship

    # process movies_metadata.csv
    m_df = pd.read_csv(data_path / "movies_metadata.csv")
    m_df = m_df[~m_df["id"].duplicated()]
    m_df["genres"] = m_df["genres"].map(load_and_dump_json)
    invalid_budget_rows = m_df[m_df["budget"].str.contains(".jpg")].index
    m_df.drop(invalid_budget_rows, axis=0, inplace=True)
    m_df.to_csv(data_path / "movies.csv", index=False)

    logger.info("Done! ✅🎉")


if __name__ == "__main__":
    main()
