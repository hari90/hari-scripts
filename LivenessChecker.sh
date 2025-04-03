#!/bin/bash
# This script runs the LivenessChecker Java program with the required arguments

java -cp .:postgresql-42.7.5.jar LivenessChecker "$@"
