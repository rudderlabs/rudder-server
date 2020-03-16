import { action, computed, observable, reaction, trace } from 'mobx';

export interface IMessageStore {
  isError: boolean;
  isSuccess: boolean;
  infoString: string;
  setError(value: boolean): void;
  setSuccess(value: boolean): void;
  setInfoString(value: string): void;
  showSuccessMessage(value: string): void;
  showErrorMessage(value: string): void;
  removeSuccessMessage(): void;
  removeErrorMessage(): void;
}

export class MessagesStore implements IMessageStore {
  @observable public isError: boolean;
  @observable public isSuccess: boolean;
  @observable public infoString: string;

  constructor() {
    this.isError = false;
    this.isSuccess = false;
    this.infoString = '';
  }

  @action.bound
  public setError(value: boolean): void {
    this.isError = value;
  }

  @action.bound
  public setSuccess(value: boolean): void {
    this.isSuccess = value;
  }

  @action.bound
  public setInfoString(value: string): void {
    this.infoString = value;
  }

  @action.bound
  public showSuccessMessage(infoString: string) {
    this.setInfoString(infoString);
    this.setSuccess(true);
    setTimeout(() => {
      if (this.isSuccess === true) {
        this.setInfoString('');
        this.setSuccess(false);
      }
    }, 3000);
  }

  @action.bound
  public removeSuccessMessage() {
    this.setSuccess(false);
  }

  @action.bound
  public showErrorMessage(infoString: string) {
    this.setInfoString(infoString);
    this.setError(true);
    setTimeout(() => {
      if (this.isError === true) {
        this.setInfoString('');
        this.setError(false);
      }
    }, 3000);
  }

  @action.bound
  public removeErrorMessage() {
    this.setError(false);
  }
}
