import ast
import os
from pathlib import Path

import dotenv
import pandas as pd
from tqdm.auto import tqdm

tqdm.pandas()

dotenv.load_dotenv()


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
    data_path = Path(os.environ.get("DATA_PATH", None))
    if not data_path.exists():
        raise ValueError(f"Invalid path to data: {str(data_path)}")

    df = pd.read_csv(data_path / "credits.csv")
    df = df.progress_apply(move_movie_id_in_jsons, axis=1)
    print("Almost done... 😴")
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

    cast_info.to_csv("./data/cast_info.csv", index=False)
    cast_movie_relationship.to_csv("./data/cast_movie_relationship.csv", index=False)
    crew_info.to_csv("./data/crew_info.csv", index=False)
    crew_movie_relationship.to_csv("./data/crew_movie_relationship.csv", index=False)
    print("Done! ✅🎉")


if __name__ == "__main__":
    main()
