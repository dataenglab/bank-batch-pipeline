#!/bin/bash
echo " Testing Bank Batch Pipeline Setup"
echo "======================================"

echo "1. Checking current directory..."
pwd

echo ""
echo "2. Checking project files..."
ls -la

echo ""
echo "3. Checking data folder..."
ls -la data/

echo ""
echo "4. Checking if CSV file exists..."
if [ -f "data/bank_transactions.csv" ]; then
    echo " CSV file found!"
    file_size=$(du -h "data/bank_transactions.csv" | cut -f1)
    echo "   File size: $file_size"
else
    echo " CSV file not found!"
fi

echo ""
echo "5. Checking Docker..."
docker --version
docker-compose --version

echo ""
echo " Test completed!"
