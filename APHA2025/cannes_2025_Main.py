
from healthapp.APHA2025.mainCannes import (
    apha_followers_count_analysis,
    apha_actual_projected_followers_percentage,
    followers_comparison_2024_2025,
    apha_2025_Articles_Analysis_Graph,
    get_selected_columns_in_date_range_FBPost,
    get_selected_columns_in_date_range_InstaPost,
    get_selected_YoutubeVideosHealth,
    apha_Articles_Analysis,
    hashtag_analysis,
    coreFollowers,
    current_followers_data,
    dod_followers_change_count,
    apha_followers_cumulative_analysis,
)
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.cache import cache
from django.shortcuts import render
import logging
from datetime import date, timedelta

# Set up logging
logger = logging.getLogger(__name__)

def insights_analysis(preApha=None, postApha=None, Apha=None):
    """Collect insights data with threading."""
    results = {}

    # Define tasks to execute in parallel
    tasks = {
        'fb': (get_selected_columns_in_date_range_FBPost, preApha, postApha, Apha),
        'insta': (get_selected_columns_in_date_range_InstaPost, preApha, postApha, Apha),
        'yt': (get_selected_YoutubeVideosFilm, preApha, postApha, Apha),
    }

    # Execute tasks in parallel with a timeout
    with ThreadPoolExecutor() as executor:
        future_to_key = {executor.submit(fn, *args): key for key, (fn, *args) in tasks.items()}
        for future in as_completed(future_to_key):
            key = future_to_key[future]
            try:
                results[key] = future.result()
            except Exception as exc:
                logger.error(f"Task {key} with args {tasks[key][1:]} generated an exception: {exc}", exc_info=True)

    # Unpack and process the results
    fb_results, fb_insights = results.get('fb', (None, None))
    insta_results, insta_insights = results.get('insta', (None, None))
    yt_results = results.get('yt', None)
    # Perform hashtag analysis based on retrieved insights
    hashtag_results = hashtag_analysis(fb_insights, insta_insights)

    return {
        'fb_results': fb_results,
        'insta_results': insta_results,
        'yt_results': yt_results,
        'hashtag_results': hashtag_results,
    }

def initial_data(preApha=None, postApha=None, Apha=None):
    """
    Process the input data and return the analysis context.
    Args:
        preApha (bool): Flag for pre-APHA data.
        postApha (bool): Flag for post-APHA data.
        Apha (int): Integer representing APHA day.
    """
    # # Default value if no arguments are provided
    # if all(arg is None for arg in [preApha, postApha, Apha]):
    #     Apha = 1

    # if Apha is not None:
    #     Apha = int(Apha)
    today = date.today()
    print(f"[DEBUG] Today's date: {today}")

    # APHA 2025 start date — adjust this as needed
    APHA_START_DATE = date(2025, 11, 2)
    print(f"[DEBUG] APHA start date: {APHA_START_DATE}")

    # Calculate current APHA day
    if today >= APHA_START_DATE:
        current_apha_day = (today - APHA_START_DATE).days + 1
    else:
        current_apha_day = 0  # Pre-APHA
    print(f"[DEBUG] Current APHA day: {current_apha_day}")

    # Decide defaults if no filter is passed
    if all(arg is None for arg in [preApha, postApha, Apha]):
        if current_apha_day <= 1:
            preApha = True
            print(f"[DEBUG] Defaulting to preApha=True (Day 1 or earlier)")
        else:
            Apha = current_apha_day - 1
            Apha = 4  # Default to showing previous day if no filters are set 
            print(f"[DEBUG] Defaulting to APHA={Apha} (showing previous day)")

    # Validate and adjust passed APHA day
    if Apha is not None:
        try:
            Apha = int(Apha)
            print(f"[DEBUG] APHA param received: {Apha}")
            if Apha >= current_apha_day:
                print(f"[DEBUG] APHA day {Apha} is today or future, adjusting to previous day")
                Apha = max(1, current_apha_day - 1)
        except ValueError:
            print(f"[DEBUG] Invalid APHA value provided: {Apha}, resetting to None")
            Apha = None

    print(f"[DEBUG] Final parameters: preApha={preApha}, postApha={postApha}, Apha={Apha}")

    # Construct a unique cache key
    cache_key = f"initial_data_{preApha}_{postApha}_{Apha}"
    data = cache.get(cache_key)

    if not data:
        logger.info(f"[Cache MISS] Fetching new data for {cache_key}")
        tasks = {
            'current_followers_data': (current_followers_data, preApha, postApha, Apha),
            'followers_count_analysis': (apha_followers_count_analysis, preApha, postApha, Apha),
            'actual_projected_followers_analysis': (apha_actual_projected_followers_percentage, preApha, postApha, Apha),
            'followers_comparison_2024_2025': (followers_comparison_2024_2025, preApha, postApha, Apha),
            'post_insights_and_hashtags': (insights_analysis, preApha, postApha, Apha),
            'articles_analysis': (apha_2025_Articles_Analysis_Graph, preApha, postApha, Apha),
            'articles_analysis_table': (apha_Articles_Analysis, preApha, postApha, Apha),
            'core_followers': (coreFollowers, preApha, postApha, Apha),
            'dod_followers_change_count': (dod_followers_change_count, preApha, postApha, Apha),
            'cumulative_followers_change': (apha_followers_cumulative_analysis, preApha, postApha, Apha),
        }


        data = {}
        try:
            with ThreadPoolExecutor() as executor:
                future_to_key = {
                    executor.submit(fn, *args): key
                    for key, (fn, *args) in tasks.items()
                }
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        data[key] = future.result()
                    except Exception as exc:
                        logger.error(f"Task {key} generated an exception: {exc}", exc_info=True)
        except RuntimeError as e:
            logger.critical(f"[ThreadPoolExecutor Error] Cannot schedule futures: {e}", exc_info=True)

        # Cache data for 15 minutes
        cache.set(cache_key, data, timeout=60 * 15)

    # Prepare context
    context = {
        'hello_world': f"data for {preApha}, {postApha}, {Apha}, data type of {type(Apha)}",
        'current_followers': data.get('current_followers_data', "None"),
        'followers_count_analysis': data.get('followers_count_analysis', "None"),
        'followers_comparison_analysis': data.get('followers_comparison_2024_2025', "None"),
        'actual_projected_followers_analysis': data.get('actual_projected_followers_analysis', "None"),
        'post_insights_and_hashtags': data.get('post_insights_and_hashtags', "None"),
        'articles_analysis': data.get('articles_analysis') if data.get('articles_analysis') not in [None, {}, []] else (print("[DEBUG] articles_analysis is empty or None, setting to 'None'") or "None"),
        'articles_analysis_table': data.get('articles_analysis_table') if data.get('articles_analysis_table') not in [None, {}, []] else (print("[DEBUG] articles_analysis_table is empty or None, setting to 'None'") or "None"),
        'core_followers': data.get('core_followers', "None"),
        'followers_comparison_2024_2025': data.get('followers_comparison_2024_2025', "None"),
        'dod_followers_comparison_2024_2025': data.get('dod_followers_change_count', "None"),
        'cumulative_followers_change': data.get('cumulative_followers_change', "None")
    }

    return context
