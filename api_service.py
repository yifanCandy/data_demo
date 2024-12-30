from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import Optional, List
from db_service import *
from typing import Optional
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

class APIService:
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger('APIService')
        self.logger.info("init class APIService.")
        self.app = FastAPI()

        # 配置 CORS 中间件
        self.app.add_middleware(
                CORSMiddleware,
                allow_origins=[
                    "http://localhost",
                    "http://localhost:3000",
                    "https://localhost:3000",
                    "http://127.0.0.1:3000",
                    "https://127.0.0.1:3000",
                    "*"
                ],
                allow_credentials=True,
                allow_methods=["*"],
                allow_headers=["*"],
                expose_headers=["*"]
        )

        # Add request logging middleware
        @self.app.middleware("http")
        async def log_requests(request: Request, call_next):
            try:
                self.logger.info(f"Request: {request.method} {request.url}")
                response = await call_next(request)
                self.logger.info(f"Response status: {response.status_code}")
                return response
            except Exception as e:
                self.logger.error(f"Request error: {str(e)}")
                return JSONResponse(
                    status_code=500, 
                    content={"message": "Internal server error"}
                )
         # Add global exception handler
        @self.app.exception_handler(Exception)
        async def global_exception_handler(request: Request, exc: Exception):
            self.logger.error(f"Unhandled exception: {str(exc)}")
            return JSONResponse(
                status_code=500,
                content={"message": "Unexpected error occurred"}
            )

        self.items = []
        self.setup_routes()
        self.db_service_ins = Db_Service(self.logger)
        self.logger.info("init class APIService end.")

    def setup_routes(self):

        @self.app.on_event("startup")
        async def startup_event():
            self.logger.info("Application is starting up")

        @self.app.on_event("shutdown")
        async def shutdown_event():
            self.logger.info("Application is shutting down")

        @self.app.post("/items/", response_model=Item)
        def create_item(item: Item):
            return Item()

        @self.app.get("/hello/")
        async def hello():
            return {"message": "Hello from FastAPI"}

        @self.app.get("/root_info/", response_model=List[Item])
        def get_root_node():
            self.logger.info("enter into func get_root_node.")
            res = []
            properties_rep, approve_prop = self.db_service_ins.get_root_info()
            properties_ins = Properties(**properties_rep)
            properties_ins_approve = Properties(**approve_prop)
            item = Item(id=self.db_service_ins.root_uid, name=self.db_service_ins.root_name, properties=properties_ins)
            item_approve = Item(id=self.db_service_ins.root_approve_uid, name=self.db_service_ins.root_approve_name, properties=properties_ins_approve)
            res.append(item)
            res.append(item_approve)
            self.logger.info(f"end func get_root_node. repository uid:{item.id}. approve uid:{item_approve.id}")
            return res

        @self.app.get("/get_childs/{uid}", response_model=List[Item])
        def get_children(uid: str):
            self.logger.info("enter into func get_children.")
            children_uid_set = self.db_service_ins.query_children_uid_set(uid)
            res = []
            if not children_uid_set:
               return res
            uid_name_map = self.db_service_ins.query_uid_and_name(children_uid_set)
            for item_uid in children_uid_set:
                properties_ins = Properties(**self.db_service_ins.query_object_info(item_uid))
                item = Item(
                    id=item_uid,
                    name=uid_name_map.get(item_uid, ""),
                    properties = properties_ins
                    )
                res.append(item)
                self.logger.info(f"uid:{uid}, children:{item}")
            self.logger.info(f"end func get_children")
            return res
            

        @self.app.get("/items/{item_id}", response_model=Item)
        def get_item(item_id: str):
            return item
            # raise HTTPException(status_code=404, detail="Item not found")

        @self.app.put("/items/{item_id}", response_model=Item)
        def update_item(item_id: int, updated_item: Item):
            return item
            # raise HTTPException(status_code=404, detail="Item not found")

# 数据模型
class Properties(BaseModel):
    class_info: Optional[dict[str, str]] = None
    addon_info: Optional[dict[str, str]] = None
    spec_history_info: Optional[dict[str, str]] = None
    iquavis_info: Optional[dict[str, str]] = None
    follow_status: Optional[dict[str, str]] = None
    file_info: Optional[dict[str, str]] = None
    
    approval_info: Optional[List[dict[str, str]]] = None
    follow_info: Optional[List[dict[str, str]]] = None
    notice_receive_info: Optional[List[dict[str, str]]] = None
    notice_refusal_info: Optional[List[dict[str, str]]] = None

class Item(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    properties: Optional[Properties] = None
    children: Optional[List["Item"]] = None
