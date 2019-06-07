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
    this.hourByHour = [];
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
    debugger;
    for (let i = 0; i < this.battery.stateOfCharge; i++) {
      const marginalHour = i 
      this.findNextMax(0, marginalHour);
      // this.battery.marginalCashFlow.push({
      //   value: this.battery.calcAccountBalance(marginalHour),
      //   firstMove: 'sell'
      // });
      this.exportCurrentCapacityHour();
    }
    // Buy first action - spare capacity in battery
    for (let i = 0; i < this.battery.batteryLim - this.battery.stateOfCharge; i++) {
      const marginalHour = i + this.battery.stateOfCharge;
      this.findNextMin(0, marginalHour);
      // this.battery.marginalCashFlow.push({
      //   value: this.battery.calcAccountBalance(marginalHour),
      //   firstMove: 'buy'
      // })
      this.exportCurrentCapacityHour();
    }
    debugger;
    // this.exportPlan();
  }
  exportPlan(){
    const dataToExport = {
      log: this.hourByHour,
      accountBalance: this.battery.calcAccountBalance(),
      stateOfCharge: this.battery.calcStateOfCharge()
    };
    fs.writeFileSync(outputFileName, JSON.stringify(this.hourByHour))
  }
  findNextMax(i, marginalHour){ // presumes upward slope
    while (i < this.remainingHours.length && !this.isMax(i)) {
      i++
    }
    this.sell(i, marginalHour); 

    if (i + 1 < this.remainingHours.length) {
      this.findNextMin(i, marginalHour);
    } 
  }
  findNextMin(i, marginalHour){
    while (i < this.remainingHours.length && !this.isMin(i)) {
      i++
    }
    this.buy(i, marginalHour);

    if (i + 1 < this.remainingHours.length) {
      this.findNextMax(i, marginalHour);
    }
  }
  isMax(i) { 
    // pointer issues with i?
    // Can I assume that i'm on an upward trajectory, and call it max if the next one is less?
    // const prevPrice = (i - 1 >= 0) ? this.remainingHours[i - 1].price : 0;
    const price = this.remainingHours[i].price;
    // Find index of next unassigned hour
    i++
    while (i < this.remainingHours.length && this.remainingHours[i].transaction !== undefined) {
      i++
    }
    // check not at end
    const nextPrice = (i < this.remainingHours.length) ? this.remainingHours[i].price : 0;
    return parseFloat(price) > parseFloat(nextPrice);
  }
  isMin(i) {
    // const prevPrice = (i - 1 >= 0) ? this.remainingHours[i - 1].price : Infinity;
    const price = this.remainingHours[i].price;
    // Find index of next unassigned hour
    debugger;
    i++
    while (i < this.remainingHours.length && this.remainingHours[i].transaction !== undefined) {
      i++
    }

    // Grab price, or if at end, assume infinity (still good assumption?)
    const nextPrice = (i < this.remainingHours.length) ? this.remainingHours[i].price : Infinity;
    return parseFloat(price) < parseFloat(nextPrice);
  }
  buy(index, marginalHour){
    console.log(`bought @ index ${index}, marginalHour ${marginalHour}, price ${price}`)
    this.remainingHours[index] = {
      ...this.remainingHours[index],
      transaction: 'buy',
      marginalHour: marginalHour
    }
  }
  sell(index, marginalHour){
    console.log(`sold @ index ${index}, marginalHour ${marginalHour}, price ${price}`)
    this.remainingHours[index] = {
      ...this.remainingHours[index],
      transaction: 'sell',
      marginalHour: marginalHour
    }
  }
}


const algorithm = (priceDataArray, stateOfCharge = 0, batterylim = 3)=>{
  let battery = new Battery(stateOfCharge, batterylim)
  let priceScheme = new PriceScheme(battery, `${filepath}/${inputFileName}`, priceDataArray); // should be able to get rid of the filepath/local stuff
  // priceScheme.import();
  priceScheme.plan();
  battery.sortLog();
}

module.exports = algorithm;