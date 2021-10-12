db.createUser(
    {
        user: "nycbuswatcher",
        pwd: "bustime",
        roles: [
            {
                role: "readWrite",
                db: "nycbuswatcher"
            }
        ]
    }
);