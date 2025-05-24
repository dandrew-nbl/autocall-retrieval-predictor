from pydantic import BaseModel, Field
from typing import List, Optional

class PredictionRequest(BaseModel):
    """Schema for prediction request"""
    item_type: str = Field(..., description="Item type (SHRF or LABL)")
    production_line: str = Field(..., description="Production line (LOU1, LOU2, etc.)")
    cases_produced: float = Field(..., description="Forecasted total cases produced")
    cases_shipped: float = Field(..., description="Forecasted total cases shipped")
    day_of_week: int = Field(..., description="Day of week (0=Monday, 6=Sunday)")
    hour_of_day: int = Field(..., description="Hour of day (0-23)")
    n_simulations: Optional[int] = Field(500, description="Number of Monte Carlo simulations")

class ForecastRequest(BaseModel):
    """Schema for forecast request"""
    day_of_week: int = Field(..., description="Day of week (0=Monday, 6=Sunday)")
    shipping_forecast: float = Field(..., description="Forecasted total cases shipped")
    production_forecast: float = Field(..., description="Forecasted total cases produced")
    lines: Optional[List[str]] = Field(None, description="Production lines to check")
    materials: Optional[List[str]] = Field(None, description="Materials to check")
    hours: Optional[List[int]] = Field(None, description="Hours to check")
