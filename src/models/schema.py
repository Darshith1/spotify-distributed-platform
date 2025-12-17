from pydantic import BaseModel, Field, field_validator
from typing import Optional

class SpotifyTrack(BaseModel):
    """
    This model defines the strict schema for our Clean Data.
    Any data that doesn't fit this shape will be rejected or fixed.
    """
    track_id: str
    artist_name: str
    track_name: str
    year: int = Field(..., ge=1900, le=2030)  # Year must be between 1900 and 2030
    genre: str
    
    # Audio features (0.0 to 1.0 range usually, except Loudness/Tempo)
    danceability: float = Field(..., ge=0.0, le=1.0)
    energy: float = Field(..., ge=0.0, le=1.0)
    loudness: float
    valence: float = Field(..., ge=0.0, le=1.0)
    tempo: float = Field(..., gt=0.0)  # Tempo must be positive
    duration_ms: int = Field(..., gt=0) # Duration must be positive
    
    # Optional fields (might be null in bad data)
    popularity: Optional[int] = 0

    @field_validator('artist_name', 'track_name')
    def clean_text(cls, v):
        """Standardizes text: strips whitespace and title-cases."""
        if not v:
            return "Unknown"
        return v.strip().title()

    @field_validator('genre')
    def clean_genre(cls, v):
        """Ensures genre is lowercase for consistent grouping."""
        return v.strip().lower()