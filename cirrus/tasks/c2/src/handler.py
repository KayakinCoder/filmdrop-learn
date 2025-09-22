#!/usr/bin/env python3
"""
Planetary Computer to S3 Indexer

This task downloads Sentinel-2 L2A data from Microsoft Planetary Computer,
updates asset links to point to S3 storage, and prepares items for STAC
server ingestion. Designed to work with the Cirrus framework.
"""

import json
import os
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

import boto3
import requests
from pystac_client import Client
from pystac import Item
import planetary_computer


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
COLLECTION = os.getenv("PLANETARY_COMPUTER_COLLECTION", "sentinel-2-l2a")
STAC_API_URL = os.getenv(
    "STAC_API_URL", "https://planetarycomputer.microsoft.com/api/stac/v1"
)
API_MODIFIER = planetary_computer.sign_inplace
CIRRUS_DATA_BUCKET = os.getenv("CIRRUS_DATA_BUCKET")
CIRRUS_PAYLOAD_BUCKET = os.getenv("CIRRUS_PAYLOAD_BUCKET")
TASK_NAME = "planetary-computer-to-s3"

# AWS client
s3_client = boto3.client("s3")


def lambda_handler(
    event: Dict[str, Any], context: Optional[Any] = None
) -> Dict[str, Any]:
    """
    Main Lambda handler for processing Cirrus payloads.

    Downloads data from Planetary Computer, updates links to S3, and prepares
    items for STAC server ingestion via Cirrus framework.

    Args:
        event: Cirrus process payload
        context: Lambda context (optional)

    Returns:
        Updated Cirrus payload with processed features or S3 URL reference
    """
    try:
        logger.info(f"Processing event: {json.dumps(event, default=str)}")

        # Extract configuration from the payload
        process_config = event["process"][0]
        task_config = process_config.get("tasks", {}).get(
            "planetary-computer-to-s3", {}
        )

        # Get required parameters
        tile = task_config.get("tile")
        date = task_config.get("date")
        download_assets = task_config.get("download_assets", False)
        max_items = task_config.get("max_items", 100)

        if not tile or not date:
            raise ValueError(
                "Both 'tile' and 'date' are required in task configuration"
            )

        logger.info(
            f"Processing tile: {tile}, date: {date}, download_assets: {download_assets}, max_items: {max_items}"
        )

        # Query Planetary Computer for STAC items
        stac_items = query_planetary_computer(tile, date, max_items, download_assets)
        logger.info(f"Found {len(stac_items)} STAC items")

        # Process items based on configuration
        processed_features = []
        for item in stac_items:
            if download_assets:
                # Download assets to S3 and update hrefs
                updated_item = download_and_update_item(item)
            else:
                # Just update metadata for indexing
                updated_item = prepare_item_for_indexing(item)

            processed_features.append(updated_item.to_dict())

        # Update the payload with processed features
        if "features" not in event:
            event["features"] = []
        event["features"].extend(processed_features)

        logger.info(f"Successfully processed {len(processed_features)} features")

        # Check if payload exceeds Step Functions limit and upload to S3 if needed
        if should_upload_to_s3(event):
            s3_url = upload_payload_to_s3(event)
            # Return minimal payload with URL reference (Cirrus pattern)
            return {"url": s3_url}
        else:
            # Return full payload if small enough
            return event

    except Exception as e:
        logger.error(f"Error processing payload: {str(e)}")
        # Add error to payload for debugging
        if "errors" not in event:
            event["errors"] = []
        event["errors"].append(
            {
                "task": "planetary-computer-to-s3",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }
        )
        raise


def query_planetary_computer(
    tile: str, date: str, max_items: int = 100, download_assets: bool = False
) -> List[Item]:
    """
    Query Planetary Computer for Sentinel-2 L2A data.

    Args:
        tile: MGRS tile identifier (e.g., '33UUP')
        date: Date string in format 'YYYY-MM-DD' or 'YYYY-MM-DD/YYYY-MM-DD'
        max_items: Maximum number of items to return (safety limit)
        download_assets: Whether assets will be downloaded (affects logging)

    Returns:
        List of STAC items (limited by max_items)
    """
    logger.info(
        f"Querying Planetary Computer for tile {tile} on {date}, max_items: {max_items}"
    )

    try:
        client = Client.open(STAC_API_URL, modifier=API_MODIFIER)

        # Handle date range or single date
        if "/" in date:
            datetime_query = date
        else:
            # Convert single date to a day range
            start_date = datetime.fromisoformat(date)
            end_date = start_date + timedelta(days=1)
            datetime_query = f"{start_date.isoformat()}/{end_date.isoformat()}"

        search = client.search(
            collections=[COLLECTION],
            datetime=datetime_query,
            query={"s2:mgrs_tile": {"eq": tile}},
            max_items=max_items,  # Limit results for safety
        )

        items = list(search.items())

        logger.info(f"Found {len(items)} items for tile {tile}")

        # Log appropriate message based on download_assets setting
        if download_assets:
            estimated_size_gb = len(items) * 0.5  # ~500MB per Sentinel-2 scene
            logger.info(
                f"Estimated download size: {estimated_size_gb:.1f} GB (download_assets=true)"
            )

            if estimated_size_gb > 10:
                logger.warning(
                    f"Large dataset detected ({estimated_size_gb:.1f} GB). Consider reducing date range or max_items."
                )
        else:
            logger.info(f"Processing metadata only (download_assets=false)")

        return items

    except Exception as e:
        logger.error(f"Error querying Planetary Computer: {str(e)}")
        raise


def download_and_update_item(item: Item) -> Item:
    """
    Download GeoTIFF assets to S3 and update item hrefs.

    Args:
        item: Original STAC item from Planetary Computer

    Returns:
        Updated STAC item with S3 hrefs
    """
    logger.info(f"Downloading assets for item: {item.id}")

    # Sign the item to get proper download URLs
    signed_item = planetary_computer.sign(item)

    for asset_key, asset in signed_item.assets.items():
        if asset.media_type and "geotiff" in asset.media_type:
            try:
                # Download the asset using the signed URL
                response = requests.get(asset.href, timeout=300)
                response.raise_for_status()

                # Generate S3 key
                s3_key = f"sentinel-2-l2a/{item.id}/{asset_key}.tif"

                # Upload to S3
                s3_client.put_object(
                    Bucket=CIRRUS_DATA_BUCKET,
                    Key=s3_key,
                    Body=response.content,
                    ContentType=asset.media_type,
                )

                # Update asset href in original item
                item.assets[asset_key].href = f"s3://{CIRRUS_DATA_BUCKET}/{s3_key}"
                logger.info(f"Downloaded and updated {asset_key} for {item.id}")

            except Exception as e:
                logger.warning(
                    f"Failed to download asset {asset_key} for {item.id}: {str(e)}"
                )
                # Keep original href if download fails

    return item


def prepare_item_for_indexing(item: Item) -> Item:
    """
    Prepare STAC item for indexing without downloading assets.

    Updates metadata and collection information to ensure proper indexing
    in the STAC server.

    Args:
        item: Original STAC item from Planetary Computer

    Returns:
        STAC item prepared for indexing
    """
    # Ensure collection is set properly
    # item.collection = "sentinel-2-l2a"

    # Update links to remove unnecessary ones and mark canonical
    links_to_remove = []
    for link in item.links:
        if link.rel == "self":
            link.rel = "canonical"
        elif link.rel not in ["canonical", "collection", "root"]:
            links_to_remove.append(link)

    for link in links_to_remove:
        item.links.remove(link)

    return item


def upload_payload_to_s3(payload: Dict[str, Any]) -> str:
    """
    Upload payload to S3 and return the URL.

    This follows the Cirrus pattern for handling large payloads that exceed
    Step Functions' 256KB limit.

    Args:
        payload: The payload dictionary to upload

    Returns:
        S3 URL where the payload was uploaded
    """
    if not CIRRUS_PAYLOAD_BUCKET:
        raise ValueError("CIRRUS_PAYLOAD_BUCKET environment variable not set")

    try:
        url = f"s3://{CIRRUS_PAYLOAD_BUCKET}/{TASK_NAME}/{payload['id']}.json"
        s3_key = f"{TASK_NAME}/{payload['id']}.json"
        s3_client.put_object(
            Bucket=CIRRUS_PAYLOAD_BUCKET,
            Key=s3_key,
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info(f"Uploaded payload to S3: {url}")
        return url

    except Exception as e:
        logger.error(f"Failed to upload payload to S3: {str(e)}")
        raise


def should_upload_to_s3(payload: Dict[str, Any]) -> bool:
    """
    Determine if payload should be uploaded to S3 based on size.

    Args:
        payload: The payload to check

    Returns:
        True if payload should be uploaded to S3, False otherwise
    """
    try:
        payload_json = json.dumps(payload, default=str)
        payload_size = len(payload_json.encode("utf-8"))

        # Upload to S3 if payload is larger than 240KB (leaving buffer below 256KB limit)
        threshold = 240000  # 240KB
        should_upload = payload_size > threshold

        logger.info(
            f"Payload size: {payload_size} bytes, threshold: {threshold} bytes, upload_to_s3: {should_upload}"
        )
        return should_upload

    except Exception as e:
        logger.warning(
            f"Could not determine payload size, defaulting to S3 upload: {str(e)}"
        )
        return True
