// const csv=require('csvtojson');
const moment=require('moment');
const fs = require('fs');

const filepath = process.env.FILEPATH || process.env.PWD;
const inputFileName = process.env.INPUTFILENAME || 'dam.csv';
const outputFileName = process.env.OUTPUTFILENAME || 'results.json';


class Battery {
  constructor(stateOfCharge = 0, batteryLim = 3) {
    this.stateOfCharge = stateOfCharge // hours of charge
    this.batteryLim = batteryLim; // hours
    this.initialMoney = 0;
    this.marginalCashFlow = [];
    this.priceSchemes = [];
    this.log = [];
  }
  sortLog(){
    this.log = this.log.sort((prev, next)=> prev.operatingHour-next.operatingHour)
  }
  calcAccountBalance(marginalHour){
    const filtered = this.log.filter( hour => marginalHour === undefined || hour.marginalHour === marginalHour)
    const prices = filtered.map( hour => hour.price);
    return -1 * prices.reduce((accumulator, curr) => {
      return accumulator + curr})
  }
  calcStateOfCharge(){
    const sorted = this.log.sort((prev, curr) => prev.operatingHour - curr.operatingHour);
    const mapped = sorted.map(hour => {
      if (hour.transaction === 'buy') {
        return 1
      } else if (hour.transaction === 'sell') {
        return -1
      }
    });
    return mapped.reduce((accumulator, curr, index)=> {
      const sum = accumulator + curr;
      if (sum < 0 || sum > this.batteryLim) {
        console.warn(`state of charge out of bounds. Value of ${sum} at index ${this.log[index].index} and operating hour ${this.log[index].operatingHour}`);
      }
      return sum
    }, this.stateOfCharge);
  }
}


class PriceScheme {
  constructor(battery, path, data = []) {
    this.path = path;
    this.battery = battery;
    this.timeLength = 1; // hours - need to implement
    this.remainingHours = data;
  }

  import(){
    csv()
      .fromFile(this.path)
      .then((jsonObj)=>{ 
        this.formatImport(jsonObj);

    })
  }
  formatImport(jsonObj) {
    const energyPrice = jsonObj.filter((hour) => {
      return hour.XML_DATA_ITEM === 'LMP_ENE_PRC'
    })
    const simplified = energyPrice.map((hour, index) => {
      const startTime = moment(hour.INTERVALSTARTTIME_GMT);
      const endTime = moment(hour.INTERVALENDTIME_GMT);
      return {
        operatingHour: parseInt(hour.OPR_HR),
        startTime: startTime.toObject(),
        endTime: endTime.toObject(),
        price: parseFloat(hour.MW),
        duration: moment.duration(endTime.diff(startTime)).as("minutes")
      }
    });
    this.remainingHours = simplified.sort((prev, curr) => prev.operatingHour - curr.operatingHour);

    // console.log(JSON.stringify(this.remainingHours))
    // this.plan();
    // console.log(this.battery.calcAccountBalance());
    // console.log(this.battery.calcStateOfCharge());
    // this.battery.sortLog();
    // let twentyFourHours = this.remainingHours.concat(this.battery.log).sort((prev, curr)=> prev.operatingHour - curr.operatingHour);
    // console.log(JSON.stringify(twentyFourHours));
  }
  exportCurrentCapacityHour(){
    let twentyFourHours = this.remainingHours.concat(this.battery.log).sort((prev, curr)=> prev.operatingHour - curr.operatingHour);
    this.hourByHour.push(twentyFourHours);
  }
  plan(){ 
    // Sell first action - energy in battery
    for (let i = 0; i < this.battery.stateOfCharge; i++) {
      const marginalHour = i 
      this.findNextMax(0, marginalHour);
    }
    // Buy first action - spare capacity in battery
    for (let i = 0; i < this.battery.batteryLim - this.battery.stateOfCharge; i++) {
      const marginalHour = i + this.battery.stateOfCharge;
      this.findNextMin(0, marginalHour);
    }
    return this.remainingHours
  }
  exportPlan(){
    fs.writeFileSync(outputFileName, JSON.stringify(this.remainingHours))
  }
  skipHoursWithTransactions(index) {
    while (index < this.remainingHours.length && this.remainingHours[index].transaction !== undefined) {
      index++
    }
    return index;
  }
  findNextMax(currIndex, marginalHour){ // presumes upward slope
    let isMax = false;
    let price, nextPrice;
    // make sure current index doesn't have a transaction on it - if so, find the next available one
    currIndex = this.skipHoursWithTransactions(currIndex)
    // while not min, advance index. 
    while (currIndex < this.remainingHours.length && !isMax) {
      price = this.remainingHours[currIndex].price;
      // Find index of next unassigned hour
      let nextIndex = currIndex + 1;
      // skip over any hour that already has a transaction on it
      nextIndex = this.skipHoursWithTransactions(nextIndex);
      // check not at end
      nextPrice = (nextIndex < this.remainingHours.length) ? this.remainingHours[nextIndex].price : 0;
      // set isMax
      isMax = parseFloat(price) > parseFloat(nextPrice);
      if (isMax) {
        this.sell(currIndex, marginalHour); 
      }
      // advance current index to next index
      currIndex = nextIndex;
    }
    
    // Find next (opposite) inflection point. 
    if (currIndex + 1 < this.remainingHours.length) {
      this.findNextMin(currIndex, marginalHour);
    } 
  }


  findNextMin(currIndex, marginalHour){
    let isMin = false;
    let price, nextPrice;
    // make sure current index doesn't have a transaction on it - if so, find the next available one
    currIndex = this.skipHoursWithTransactions(currIndex)
    // while not min, advance index. 
    while (currIndex < this.remainingHours.length && !isMin) {
      price = this.remainingHours[currIndex].price;
      // Find index of next unassigned hour
      let nextIndex = currIndex + 1;
      // skip over any hour that already has a transaction on it
      nextIndex = this.skipHoursWithTransactions(nextIndex);
      // check not at end
      nextPrice = (nextIndex < this.remainingHours.length) ? this.remainingHours[nextIndex].price : 0;
      // set isMin
      isMin = parseFloat(price) < parseFloat(nextPrice);
      if (isMin) {
        this.buy(currIndex, marginalHour); 
      }
      // advance current index to next index
      currIndex = nextIndex;
    }

    if (currIndex + 1 < this.remainingHours.length) {
      this.findNextMax(currIndex, marginalHour);
    }
  }

  buy(index, marginalHour){
    this.remainingHours[index] = {
      ...this.remainingHours[index],
      transaction: 'buy',
      marginalHour: marginalHour
    }
  }
  sell(index, marginalHour){
    this.remainingHours[index] = {
      ...this.remainingHours[index],
      transaction: 'sell',
      marginalHour: marginalHour
    }
  }
}

const algorithm = (priceDataArray, stateOfCharge = 0, batterylim = 3)=>{
  let battery = new Battery(stateOfCharge, batterylim);
  let priceScheme = new PriceScheme(battery, `${filepath}/${inputFileName}`, priceDataArray); // should be able to get rid of the filepath/local stuff
  // priceScheme.import();
  return priceScheme.plan();
  // battery.sortLog();
}

module.exports = algorithm;