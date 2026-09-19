module.exports={testEnvironment:'node',testMatch:['<rootDir>/src/**/*.spec.ts'],transform:{'^.+\\.tsx?$':['ts-jest',{tsconfig:'tsconfig.json'}]},setupFilesAfterEnv:['<rootDir>/tests/setup.ts']};
