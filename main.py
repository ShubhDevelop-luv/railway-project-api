from fastapi import FastAPI, APIRouter
from controllers.user import app
from controllers.audio import  router as audio
from controllers.history import router as history
from controllers.transcript import router as transcript

route = FastAPI(debug=True)

# Include user routes in the main app
route.include_router(app)
route.include_router(audio)
route.include_router(history)
route.include_router(transcript)

