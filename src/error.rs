#[derive(Debug)]
pub enum ConnectionError {
    PostgresError(tokio_postgres::Error),
}

impl std::fmt::Display for EnvironmentError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnvironmentError::MissingEnvironmentVariable(e) => {
                write!(f, "EnvironmentError: {}", e)
            }
        }
    }
}

#[derive(Debug)]
pub enum EnvironmentError<'a> {
    MissingEnvironmentVariable(&'a str),
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::PostgresError(e) => write!(f, "{}", e),
        }
    }
}

#[derive(Debug)]
pub enum DriverError<'a> {
    ConnectionError(ConnectionError),
    EnvironmentError(EnvironmentError<'a>),
}

impl std::error::Error for DriverError<'_> {}

impl std::fmt::Display for DriverError<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DriverError::ConnectionError(e) => write!(f, "{}", e),
            DriverError::EnvironmentError(e) => write!(f, "{}", e),
        }
    }
}
